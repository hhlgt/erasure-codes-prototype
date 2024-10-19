#include "proxy.h"

namespace ECProject
{
  MainRepairResp Proxy::main_repair(MainRepairPlan repair_plan)
  {
    struct timeval start_time, end_time;
    double decoding_time = 0;
    double cross_cluster_time = 0;

    bool if_partial_decoding = repair_plan.partial_decoding;
    size_t block_size = repair_plan.block_size;
    auto ec = ec_factory(repair_plan.ec_type, repair_plan.cp);
    ec->init_coding_parameters(repair_plan.cp);
    int failed_num = (int)repair_plan.failed_blocks_index.size();
    int *erasures = new int[failed_num + 1];
    for (int i = 0; i < failed_num; i++) {
      erasures[i] = repair_plan.failed_blocks_index[i];
    }
    erasures[failed_num] = -1;

    if (IF_DEBUG) {
      std::cout << "[Main Proxy " << self_cluster_id_ << "] To repair ";
      for (int i = 0; i < failed_num; i++) {
        std::cout << erasures[i] << " ";
      }
      std::cout << std::endl;
    }

    auto fls_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.failed_blocks_index);
    auto svrs_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.live_blocks_index);

    auto original_lock_ptr = std::make_shared<std::mutex>();
    auto original_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto original_blocks_idx_ptr = std::make_shared<std::vector<int>>();

    auto get_from_node = [this, original_lock_ptr, original_blocks_ptr,
                          original_blocks_idx_ptr, block_size](
                          int block_idx, unsigned int block_id,
                          std::string node_ip, int node_port) mutable
    {
      std::string block_id_str = std::to_string(block_id);
      std::vector<char> tmp_val(block_size);
      read_from_datanode(block_id_str.c_str(), block_id_str.size(), 
                         tmp_val.data(), block_size, node_ip.c_str(), node_port);
      original_lock_ptr->lock();
      original_blocks_ptr->push_back(tmp_val);
      original_blocks_idx_ptr->push_back(block_idx);
      original_lock_ptr->unlock();
    };

    auto partial_lock_ptr = std::make_shared<std::mutex>();
    auto partial_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto decoding_time_ptr = std::make_shared<std::vector<double>>();

    auto get_from_proxy = [this, original_lock_ptr, original_blocks_ptr,
                           original_blocks_idx_ptr, partial_lock_ptr,
                           partial_blocks_ptr, block_size,
                           decoding_time_ptr, failed_num]
                           (std::shared_ptr<asio::ip::tcp::socket> socket_ptr) mutable
    {
      asio::error_code error;
      std::vector<unsigned char> int_buf(sizeof(int));
      asio::read(*socket_ptr, asio::buffer(int_buf, int_buf.size()), error);
      int t_cluster_id = ECProject::bytes_to_int(int_buf);
      std::vector<unsigned char> int_flag_buf(sizeof(int));
      asio::read(*socket_ptr, asio::buffer(int_flag_buf, int_flag_buf.size()), error);
      int t_flag = ECProject::bytes_to_int(int_flag_buf);
      std::string msg = "data";
      if(t_flag)
        msg = "partial";
      if (IF_DEBUG) {
        std::cout << "[Main Proxy " << self_cluster_id_ << "] Try to get "
                  << msg << " blocks from the proxy in cluster " << t_cluster_id
                  << ". " << std::endl;
      }
      if(t_flag) {  // receive partial blocks from helper proxies
        std::vector<unsigned char> int_buf_num_of_blocks(sizeof(int));
        asio::read(*socket_ptr, asio::buffer(int_buf_num_of_blocks,
            int_buf_num_of_blocks.size()), error);
        int partial_block_num = ECProject::bytes_to_int(int_buf_num_of_blocks);
        if (partial_block_num != failed_num) {
          std::cout << "Error! partial_blocks_num != failed_num" << std::endl;
        }
        partial_lock_ptr->lock();
        for (int i = 0; i < failed_num; i++) {
          std::vector<char> tmp_val(block_size);
          asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), error);
          partial_blocks_ptr->push_back(tmp_val);
        }
        partial_lock_ptr->unlock();
      } else {  // receive data blocks from help proxies
        std::vector<unsigned char> int_buf_num_of_blocks(sizeof(int));
        asio::read(*socket_ptr, asio::buffer(int_buf_num_of_blocks,
            int_buf_num_of_blocks.size()), error);
        int block_num = ECProject::bytes_to_int(int_buf_num_of_blocks);
        for (int i = 0; i < block_num; i++) {
          std::vector<char> tmp_val(block_size);
          std::vector<unsigned char> block_idx_buf(sizeof(int));
          asio::read(*socket_ptr, asio::buffer(block_idx_buf,
              block_idx_buf.size()), error);
          int block_idx = ECProject::bytes_to_int(block_idx_buf);
          asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), error);
          original_lock_ptr->lock();
          original_blocks_ptr->push_back(tmp_val);
          original_blocks_idx_ptr->push_back(block_idx);
          original_lock_ptr->unlock();
        }
        std::vector<unsigned char> decoding_time_buf(sizeof(double));
        asio::read(*socket_ptr, asio::buffer(decoding_time_buf,
                                        decoding_time_buf.size()));
        double temp_decoding_time = bytes_to_double(decoding_time_buf);
        decoding_time_ptr->push_back(temp_decoding_time);
      }

      if (IF_DEBUG){
        std::cout << "[Main Proxy " << self_cluster_id_
                  << "] Finish getting data from the proxy in cluster "
                  << t_cluster_id << std::endl;
      }
    };

    auto send_to_datanode = [this, block_size](
        unsigned int block_id, char *data, std::string node_ip, int node_port)
    {
      std::string block_id_str = std::to_string(block_id);
      write_to_datanode(block_id_str.c_str(), block_id_str.size(),
                        data, block_size, node_ip.c_str(), node_port);
    };

    // get original blocks inside cluster
    int num_of_original_blocks =
        (int)repair_plan.inner_cluster_help_blocks_info.size();
    if (num_of_original_blocks > 0) {
      std::vector<std::thread> readers;
      for (int i = 0; i < num_of_original_blocks; i++) {
        readers.push_back(std::thread(get_from_node,
            repair_plan.inner_cluster_help_blocks_info[i].first,
            repair_plan.inner_cluster_help_block_ids[i],
            repair_plan.inner_cluster_help_blocks_info[i].second.first,
            repair_plan.inner_cluster_help_blocks_info[i].second.second));
      }
      for (int i = 0; i < num_of_original_blocks; i++) {
        readers[i].join();
      }

      if (IF_DEBUG) {
        std::cout << "[Main Proxy " << self_cluster_id_
                  << "] Finish getting blocks inside main cluster." << std::endl;
      }
    }

    gettimeofday(&start_time, NULL);
    // get blocks or partial blocks from other clusters
    int num_of_help_clusters = (int)repair_plan.help_clusters_blocks_info.size();
    int partial_cnt = 0;  // num of partial-block sets
    if (num_of_help_clusters > 0) {
      std::vector<std::thread> readers;
      for (int i = 0; i < num_of_help_clusters; i++) {
        int num_of_blocks_in_cluster =
            (int)repair_plan.help_clusters_blocks_info[i].size();
        bool t_flag = true;
        if (num_of_blocks_in_cluster <= failed_num) {
          t_flag = false;
        }
        t_flag = (if_partial_decoding && t_flag);
        if(!t_flag && IF_DIRECT_FROM_NODE) {  // transfer blocks directly
          num_of_original_blocks += num_of_blocks_in_cluster;
          for (int j = 0; j < num_of_blocks_in_cluster; j++) {
            readers.push_back(std::thread(get_from_node,
                repair_plan.help_clusters_blocks_info[i][j].first, 
                repair_plan.help_clusters_block_ids[i][j],
                repair_plan.help_clusters_blocks_info[i][j].second.first,
                repair_plan.help_clusters_blocks_info[i][j].second.second));
          }
        } else {  // transfer blocks through proxies
          if(t_flag) { // encode partial blocks and transfer through proxies
            partial_cnt++;
          } else {   // direct transfer original blocks through proxies
            num_of_original_blocks += num_of_blocks_in_cluster;
          }
          std::shared_ptr<asio::ip::tcp::socket>
              socket_ptr = std::make_shared<asio::ip::tcp::socket>(io_context_);
          acceptor_.accept(*socket_ptr);
          readers.push_back(std::thread(get_from_proxy, socket_ptr));
        }
      }
      int num_of_readers = (int)readers.size();
      for (int i = 0; i < num_of_readers; i++) {
        readers[i].join();
      }

      // simulate cross-cluster transfer
      if (IF_SIMULATE_CROSS_CLUSTER) {
        for (int i = 0; i < num_of_help_clusters; i++) {
          int num_of_blocks = (int)repair_plan.help_clusters_blocks_info[i].size();
          bool t_flag = true;
          if (num_of_blocks <= failed_num) {
            t_flag = false;
          }
          t_flag = (if_partial_decoding && t_flag);
          if (t_flag) {
            num_of_blocks = failed_num;
          }
          size_t t_val_len = (int)block_size * num_of_blocks;
          std::string t_value = generate_random_string((int)t_val_len);
          transfer_to_networkcore(t_value.c_str(), t_val_len);
        }
      }

      if (decoding_time_ptr->size() > 0) {
        auto max_decode = std::max_element(decoding_time_ptr->begin(),
            decoding_time_ptr->end());
        decoding_time += *max_decode;
      }
    }
    gettimeofday(&end_time, NULL);
    cross_cluster_time += end_time.tv_sec - start_time.tv_sec +
        (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    
    my_assert(num_of_original_blocks == (int)original_blocks_ptr->size());
    if (num_of_original_blocks > 0 && if_partial_decoding) {  // encode-and-transfer
      std::vector<char *> v_data(num_of_original_blocks);
      std::vector<char *> v_coding(failed_num);
      char **data = (char **)v_data.data();
      char **coding = (char **)v_coding.data();
      for (int j = 0; j < num_of_original_blocks; j++) {
        data[j] = (*original_blocks_ptr)[j].data();
      }
      std::vector<std::vector<char>>
          v_coding_area(failed_num, std::vector<char>(block_size));
      for (int j = 0; j < failed_num; j++) {
        coding[j] = v_coding_area[j].data();
      }
            
      gettimeofday(&start_time, NULL);
      ec->encode_partial_blocks_for_decoding(data, coding, block_size,
          *original_blocks_idx_ptr, *svrs_idx_ptr, *fls_idx_ptr);
      gettimeofday(&end_time, NULL);
      decoding_time += end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
            
      partial_lock_ptr->lock();
      for (int j = 0; j < failed_num; j++) {
        partial_blocks_ptr->push_back(v_coding_area[j]);
      }
      partial_lock_ptr->unlock();
      partial_cnt++;
    }

    if (IF_DEBUG) {
      std::cout << "[Main Proxy " << self_cluster_id_
                << "] Ready to decode! " << num_of_original_blocks << " vs "
                << partial_cnt<< std::endl;
    }

    // decode
    int data_num = partial_cnt * failed_num;
    int coding_num = failed_num;
    if (!if_partial_decoding) {
      if(repair_plan.cp.local_or_column &&
         check_ec_family(repair_plan.ec_type) == LRCs) {
        data_num = (int)svrs_idx_ptr->size();
        coding_num = 1;
      } else {
        data_num = ec->k;
        coding_num = ec->m;
      }
    } else {
      my_assert(data_num == (int)partial_blocks_ptr->size());
    }

    if (IF_DEBUG) {
      std::cout << "[Main Proxy" << self_cluster_id_ << "] " << data_num
                << " " << partial_blocks_ptr->size() << " "
                << original_blocks_idx_ptr->size() << std::endl;
    }

    std::vector<char *> vt_data(data_num);
    std::vector<char *> vt_coding(coding_num);
    char **t_data = (char **)vt_data.data();
    char **t_coding = (char **)vt_coding.data();
    std::vector<std::vector<char>> vt_data_area(data_num, std::vector<char>(block_size));
    std::vector<std::vector<char>> vt_coding_area(coding_num, std::vector<char>(block_size));
    for (int i = 0; i < coding_num; i++) {
      t_coding[i] = vt_coding_area[i].data();
    }

    // for local repair
    int group_id = -1;
    int min_idx = 0;
    int group_size = 0;
    std::unordered_map<int, int> idx2idx;
    if (if_partial_decoding) {
      for (int i = 0; i < data_num; i++) {
        t_data[i] = (*partial_blocks_ptr)[i].data();
      }
    } else {
      if (repair_plan.cp.local_or_column &&
          check_ec_family(repair_plan.ec_type) == LRCs) {
        LocallyRepairableCode *lrc = dynamic_cast<LocallyRepairableCode *>(ec);
        for (auto it = svrs_idx_ptr->begin(); it != svrs_idx_ptr->end(); it++) {
          int idx = *it;
          if(idx >= lrc->k + lrc->g){
            group_id = idx - lrc->k - lrc->g;
            break;
          }
        }
        if (group_id == -1) {
          for (auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++) {
            int idx = *it;
            if(idx >= lrc->k + lrc->g){
              group_id = idx - lrc->k - lrc->g;
              break;
            }
          }
        }
        if (group_id == -1) {
          std::cout << "[Main Proxy" << self_cluster_id_ << "] error! "
                    << group_id << std::endl;
        }

        for (int i = 0; i < data_num; i++) {
          int idx = (*original_blocks_idx_ptr)[i];
          group_size = lrc->get_group_size(group_id, min_idx);
          if (idx >= lrc->k + lrc->g) {
            t_coding[0] = (*original_blocks_ptr)[i].data();
            idx2idx[idx] = group_size;
          } else {
            if (idx >= lrc->k && repair_plan.ec_type == OPTIMAL_CAUCHY_LRC) {
              t_data[group_size - lrc->g + (idx - lrc->k)] =
                (*original_blocks_ptr)[i].data();
              idx2idx[idx] = group_size - lrc->g + (idx - lrc->k);
            } else {
              int idx_in_group = lrc->idxingroup(idx);
              t_data[idx_in_group] = (*original_blocks_ptr)[i].data();
              idx2idx[idx] = idx_in_group;
            }
          }
        }  
      } else {
        for (int i = 0; i < data_num; i++) {
          int idx = (*original_blocks_idx_ptr)[i];
          if(idx >= ec->k) {
            t_coding[idx - ec->k] = (*original_blocks_ptr)[i].data();
          } else {
            t_data[idx] = (*original_blocks_ptr)[i].data();
          }
        }
      }
    }

    if (IF_DEBUG) {
      std::cout << "[Main Proxy" << self_cluster_id_
                << "] decoding!" << std::endl;
    }

    gettimeofday(&start_time, NULL);
    if (if_partial_decoding) {
      if (data_num == coding_num) {
        t_coding = t_data;
      } else {
        ec->perform_addition(t_data, t_coding, block_size, data_num, coding_num);
      }
    } else {
      if(repair_plan.cp.local_or_column &&
          check_ec_family(repair_plan.ec_type) == LRCs) {
          erasures[0] = idx2idx[erasures[0]];
          erasures[1] = group_id;
          my_assert(failed_num == 1);
      }
      ec->decode(t_data, t_coding, block_size, erasures, failed_num);
    }
    gettimeofday(&end_time, NULL);
    decoding_time += end_time.tv_sec - start_time.tv_sec +
        (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

    std::vector<std::thread> writers;
    for (int i = 0; i < failed_num; i++) {
      int index = repair_plan.failed_blocks_index[i];
      unsigned int failed_block_id = repair_plan.failed_block_ids[i];
      if (if_partial_decoding) {
        writers.push_back(std::thread(send_to_datanode,
            failed_block_id, t_coding[i],
            repair_plan.new_locations[i].second.first,
            repair_plan.new_locations[i].second.second));
      } else {
        if (repair_plan.cp.local_or_column &&
            check_ec_family(repair_plan.ec_type) == LRCs)
        {
          index = idx2idx[index];
          if (index >= group_size) {
            writers.push_back(std::thread(send_to_datanode,
                failed_block_id, t_coding[index - group_size], 
                repair_plan.new_locations[i].second.first,
                repair_plan.new_locations[i].second.second));
          } else {
            writers.push_back(std::thread(send_to_datanode,
                failed_block_id, t_data[index], 
                repair_plan.new_locations[i].second.first,
                repair_plan.new_locations[i].second.second));
          }
        } else {
          if(index >= ec->k) {
            writers.push_back(std::thread(send_to_datanode,
                failed_block_id, t_coding[index - ec->k], 
                repair_plan.new_locations[i].second.first,
                repair_plan.new_locations[i].second.second));
          } else {
            writers.push_back(std::thread(send_to_datanode,
                failed_block_id, t_data[index], 
                repair_plan.new_locations[i].second.first,
                repair_plan.new_locations[i].second.second));
          }
        }
      }
    }
    for (int i = 0; i < failed_num; i++) {
      writers[i].join();
    }

    if (IF_SIMULATE_CROSS_CLUSTER) {
      gettimeofday(&start_time, NULL);
      int cross_cluster_num = 0;
      for (int i = 0; i < (int)repair_plan.new_locations.size(); i++) {
        if(repair_plan.new_locations[i].first != self_cluster_id_) {
          cross_cluster_num++;   
        }
      }
      if (cross_cluster_num > 0) {
        int t_value_len = (int)block_size * cross_cluster_num;
        std::vector<char> t_value(t_value_len);
        transfer_to_networkcore(t_value.data(), t_value_len);
      }
      gettimeofday(&end_time, NULL);
      cross_cluster_time += end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    }

    if (IF_DEBUG) {
      std::cout << "[Main Proxy" << self_cluster_id_ << "] finish repair "
                << failed_num << " blocks! Decoding time : "
                << decoding_time << std::endl;
    }

    delete ec;

    MainRepairResp response;
    response.decoding_time = decoding_time;
    response.cross_cluster_time = cross_cluster_time;

    return response;
  }

  void Proxy::help_repair(HelpRepairPlan repair_plan)
  {
    struct timeval start_time, end_time;
    double decoding_time = 0;

    bool if_partial_decoding = repair_plan.partial_decoding;
    bool t_flag = true;
    int num_of_original_blocks = (int)repair_plan.inner_cluster_help_blocks_info.size();
    int failed_num = (int)repair_plan.failed_blocks_index.size();
    if (num_of_original_blocks <= failed_num) {
      t_flag = false;
    }
    t_flag = (if_partial_decoding && t_flag);
    if (!t_flag && IF_DIRECT_FROM_NODE) {
      return;
    }

    auto ec = ec_factory(repair_plan.ec_type, repair_plan.cp);
    ec->init_coding_parameters(repair_plan.cp);
    size_t block_size = repair_plan.block_size;

    if (repair_plan.cp.local_or_column) {
      my_assert(1 == failed_num);
    }
    auto fls_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.failed_blocks_index);
    auto svrs_idx_ptr = std::make_shared<std::vector<int>>(
        repair_plan.live_blocks_index);

    auto original_lock_ptr = std::make_shared<std::mutex>();
    auto original_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
    auto original_blocks_idx_ptr = std::make_shared<std::vector<int>>();

    auto get_from_node = [this, original_lock_ptr, original_blocks_ptr,
                          original_blocks_idx_ptr, block_size](
                          unsigned int block_id, int block_idx,
                          std::string node_ip, int node_port) mutable
    {
      std::string block_id_str = std::to_string(block_id);
      std::vector<char> tmp_val(block_size);
      read_from_datanode(block_id_str.c_str(), block_id_str.size(), 
          tmp_val.data(), block_size, node_ip.c_str(), node_port);
      original_lock_ptr->lock();
      original_blocks_ptr->push_back(tmp_val);
      original_blocks_idx_ptr->push_back(block_idx);
      original_lock_ptr->unlock();
    };
    if (IF_DEBUG) {
      std::cout << "[Helper Proxy" << self_cluster_id_
                << "] Ready to read blocks from data node!" << std::endl;
    }

    if (num_of_original_blocks > 0) {
      std::vector<std::thread> readers;
      for (int i = 0; i < num_of_original_blocks; i++) {
        readers.push_back(std::thread(get_from_node, 
                          repair_plan.inner_cluster_help_block_ids[i], 
                          repair_plan.inner_cluster_help_blocks_info[i].first,
                          repair_plan.inner_cluster_help_blocks_info[i].second.first,
                          repair_plan.inner_cluster_help_blocks_info[i].second.second));
      }
      for (int i = 0; i < num_of_original_blocks; i++) {
        readers[i].join();
      }
    }

    my_assert(num_of_original_blocks == (int)original_blocks_ptr->size());

    int value_size = 0;

    if (t_flag) {
      if (IF_DEBUG) {
        std::cout << "[Helper Proxy" << self_cluster_id_
                  << "] partial encoding with blocks " << std::endl;
        for (auto it = original_blocks_idx_ptr->begin();
             it != original_blocks_idx_ptr->end(); it++) {
          std::cout << (*it) << " ";
        }
        std::cout << std::endl;
      }
      // encode partial blocks
      std::vector<char *> v_data(num_of_original_blocks);
      std::vector<char *> v_coding(failed_num);
      char **data = (char **)v_data.data();
      char **coding = (char **)v_coding.data();
      std::vector<std::vector<char>> v_coding_area(failed_num, std::vector<char>(block_size));
      for (int j = 0; j < failed_num; j++) {
        coding[j] = v_coding_area[j].data();
      }
      for (int j = 0; j < num_of_original_blocks; j++) {
        data[j] = (*original_blocks_ptr)[j].data();
      }
      gettimeofday(&start_time, NULL);
      ec->encode_partial_blocks_for_decoding(data, coding, block_size,
          *original_blocks_idx_ptr, *svrs_idx_ptr, *fls_idx_ptr);
      gettimeofday(&end_time, NULL);
      decoding_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

      // send to main proxy
      asio::ip::tcp::socket socket_(io_context_);
      asio::ip::tcp::resolver resolver(io_context_);
      asio::error_code con_error;
      if (IF_DEBUG) {
        std::cout << "[Helper Proxy" << self_cluster_id_
                  << "] Try to connect main proxy port "
                  << repair_plan.main_proxy_port << std::endl;
      }
      asio::connect(socket_, resolver.resolve({repair_plan.main_proxy_ip,
          std::to_string(repair_plan.main_proxy_port)}), con_error);
      if (!con_error && IF_DEBUG) {
        std::cout << "[Helper Proxy" << self_cluster_id_
                  << "] Connect to " << repair_plan.main_proxy_ip
                  << ":" << repair_plan.main_proxy_port << " success!"
                  << std::endl;
      }
      std::vector<unsigned char>
          int_buf_self_cluster_id = ECProject::int_to_bytes(self_cluster_id_);
      asio::write(socket_,
          asio::buffer(int_buf_self_cluster_id, int_buf_self_cluster_id.size()));
      std::vector<unsigned char> flag_buf = ECProject::int_to_bytes(1);
      asio::write(socket_, asio::buffer(flag_buf, flag_buf.size()));
      std::vector<unsigned char> int_buf_num_of_blocks = int_to_bytes(failed_num);
      asio::write(socket_,
          asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()));
      for (int j = 0; j < failed_num; j++) {
        asio::write(socket_, asio::buffer(coding[j], block_size));
        value_size += block_size;
      }
      std::vector<unsigned char> decoding_time_buf = double_to_bytes(decoding_time);
      asio::write(socket_, asio::buffer(decoding_time_buf, decoding_time_buf.size()));
    } else if(!IF_DIRECT_FROM_NODE)  {
      // send to main proxy
      asio::ip::tcp::socket socket_(io_context_);
      asio::ip::tcp::resolver resolver(io_context_);
      asio::error_code con_error;
      if (IF_DEBUG) {
        std::cout << "[Helper Proxy" << self_cluster_id_
                  << "] Try to connect main proxy port "
                  << repair_plan.main_proxy_port << std::endl;
      }
      asio::connect(socket_, resolver.resolve({repair_plan.main_proxy_ip,
            std::to_string(repair_plan.main_proxy_port)}), con_error);
      if (!con_error && IF_DEBUG) {
            std::cout << "[Helper Proxy" << self_cluster_id_
            << "] Connect to " << repair_plan.main_proxy_ip<< ":"
            << repair_plan.main_proxy_port << " success!" << std::endl;
      }
      
      std::vector<unsigned char>
          int_buf_self_cluster_id = ECProject::int_to_bytes(self_cluster_id_);
      asio::write(socket_, asio::buffer(int_buf_self_cluster_id,
          int_buf_self_cluster_id.size()));
      std::vector<unsigned char> flag_buf = ECProject::int_to_bytes(1);
      asio::write(socket_, asio::buffer(flag_buf, flag_buf.size()));
      std::vector<unsigned char>
          int_buf_num_of_blocks = int_to_bytes((int)original_blocks_idx_ptr->size());
      asio::write(socket_,
          asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()));

      int j = 0;
      for(auto it = original_blocks_idx_ptr->begin();
              it != original_blocks_idx_ptr->end(); it++, j++) { 
        // send index and value
        int block_idx = *it;
        std::vector<unsigned char> byte_block_idx = ECProject::int_to_bytes(block_idx);
        asio::write(socket_, asio::buffer(byte_block_idx, byte_block_idx.size()));
        asio::write(socket_, asio::buffer((*original_blocks_ptr)[j], block_size));
        value_size += block_size;
      }
    }
        
    if (IF_DEBUG) {
       std::cout << "[Helper Proxy" << self_cluster_id_
       << "] Send value to proxy" <<  repair_plan.main_proxy_port 
       << "! With length of " << ". Decoding time : " << decoding_time << std::endl;
    }
  }
}