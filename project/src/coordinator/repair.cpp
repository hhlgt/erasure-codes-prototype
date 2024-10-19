#include "coordinator.h"

namespace ECProject
{
  void Coordinator::do_repair(
          std::vector<unsigned int> failed_ids, int stripe_id,
          RepairResp& response)
  {
    struct timeval start_time, end_time;
    struct timeval m_start_time, m_end_time;
    double repair_time = 0;
    double decoding_time = 0;
    double cross_cluster_time = 0;
    double meta_time = 0;
    int cross_cluster_transfers = 0;
    std::unordered_map<unsigned int, std::vector<int>> failures_map;
    check_out_failures(stripe_id, failed_ids, failures_map);
    for (auto& pair : failures_map) {
      gettimeofday(&start_time, NULL);
      gettimeofday(&m_start_time, NULL);
      find_out_stripe_partitions(pair.first);
      Stripe& stripe = stripe_table_[pair.first];
      std::vector<RepairPlan> repair_plans;
      bool flag = stripe.ec->generate_repair_plan(pair.second, repair_plans);
      if (!flag) {
        continue;
      }
      std::vector<MainRepairPlan> main_repairs;
      std::vector<std::vector<HelpRepairPlan>> help_repairs;
      if (check_ec_family(ec_schema_.ec_type) == PCs) {
        concrete_repair_plans_pc(pair.first, repair_plans, main_repairs, help_repairs);
      } else {
        concrete_repair_plans(pair.first, repair_plans, main_repairs, help_repairs);
      }
      
      if (IF_DEBUG) {
        std::cout << "Finish generate repair plan." << std::endl;
      }

      auto lock_ptr = std::make_shared<std::mutex>();

      auto send_main_repair_plan = 
          [this, main_repairs, lock_ptr, decoding_time, cross_cluster_time](
              int i, int main_cluster_id) mutable
      {
        std::string chosen_proxy = cluster_table_[main_cluster_id].proxy_ip +
            std::to_string(cluster_table_[main_cluster_id].proxy_port);
        auto resp = 
            async_simple::coro::syncAwait(proxies_[chosen_proxy]
                ->call<&Proxy::main_repair>(main_repairs[i])).value();
        lock_ptr->lock();
        decoding_time += resp.decoding_time;
        cross_cluster_time += resp.cross_cluster_time;
        lock_ptr->unlock();
        if (IF_DEBUG) {
          std::cout << "Selected main proxy " << chosen_proxy << " of cluster"
                    << main_cluster_id << ". Decoding time : " 
                    << decoding_time << std::endl;
        }
      };

      auto send_help_repair_plan = 
          [this, help_repairs](
              int i, int j, std::string proxy_ip, int proxy_port) mutable
      {
        std::string chosen_proxy = proxy_ip + std::to_string(proxy_port);
        async_simple::coro::syncAwait(proxies_[chosen_proxy]
            ->call<&Proxy::help_repair>(help_repairs[i][j]));
        if (IF_DEBUG) {
          std::cout << "Selected help proxy " << chosen_proxy << std::endl;
        }
      };

      // simulation
      if (IF_DEBUG) {
        for (int i = 0; i < int(main_repairs.size()); i++) {
          MainRepairPlan& tmp = main_repairs[i];
          std::cout << "> Failed Blocks: ";
          for (int j = 0; 
               j < int(tmp.failed_blocks_index.size()); j++) {
            std::cout << tmp.failed_blocks_index[j] << " ";
          }
          std::cout << std::endl;
          std::cout << "> Repair by Blocks: ";
          for (int jj = 0; jj < int(tmp.inner_cluster_help_blocks_info.size()); jj++) {
            std::cout << tmp.inner_cluster_help_blocks_info[jj].first << " ";
          }
          for (int j = 0; j < int(tmp.help_clusters_blocks_info.size()); j++) {
            for(int jj = 0; jj < int(tmp.help_clusters_blocks_info[j].size()); jj++) {
              std::cout << tmp.help_clusters_blocks_info[j][jj].first << " ";
            }
          }
          std::cout << std::endl;
        }
      }
      simulation_repair(main_repairs, cross_cluster_transfers);
      if (IF_DEBUG) {
        std::cout << "Finish simulation! " << cross_cluster_transfers << std::endl;
      }
      gettimeofday(&m_end_time, NULL);
      meta_time += m_end_time.tv_sec - m_start_time.tv_sec +
          (m_end_time.tv_usec - m_start_time.tv_usec) * 1.0 / 1000000;

      if (!IF_SIMULATION) {
        for (int i = 0; i < int(main_repairs.size()); i++) {
          try
          {
            MainRepairPlan& tmp = main_repairs[i];
            int failed_num = int(tmp.failed_blocks_index.size());
            unsigned int main_cluster_id = tmp.cluster_id;
            std::thread my_main_thread(send_main_repair_plan, i, main_cluster_id);
            std::vector<std::thread> senders;
            int index = 0;
            for (int j = 0; j < int(tmp.help_clusters_blocks_info.size()); j++) {
              int num_of_blocks_in_help_cluster = 
                  (int)tmp.help_clusters_blocks_info[j].size();
              my_assert(num_of_blocks_in_help_cluster == 
                  int(help_repairs[i][j].inner_cluster_help_blocks_info.size()));
              if ((IF_DIRECT_FROM_NODE && ec_schema_.partial_decoding
                   && failed_num < num_of_blocks_in_help_cluster) ||
                  !IF_DIRECT_FROM_NODE)
              {
                Cluster &cluster = cluster_table_[help_repairs[i][j].cluster_id];
                senders.push_back(std::thread(send_help_repair_plan, i, j,
                                  cluster.proxy_ip, cluster.proxy_port));
              }
            }
            for (int j = 0; j < int(senders.size()); j++) {
              senders[j].join();
            }
            my_main_thread.join();
          }
          catch(const std::exception& e)
          {
            std::cerr << e.what() << '\n';
          }
        }
      }
      gettimeofday(&end_time, NULL);
      double temp_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      repair_time += temp_time;
      if (IF_DEBUG) {
        std::cout << "Repair: total = " << repair_time << "s, latest = "
                  << temp_time << "s. Decode: total = "
                  << decoding_time << std::endl;
      }
    }
    response.decoding_time = decoding_time;
    response.cross_cluster_time = cross_cluster_time;
    response.repair_time = repair_time;
    response.meta_time = meta_time;
    response.cross_cluster_transfers = cross_cluster_transfers;
  }

  void Coordinator::check_out_failures(
          int stripe_id, std::vector<unsigned int> failed_ids,
          std::unordered_map<unsigned int, std::vector<int>> &failures_map)
  {
    if (stripe_id >= 0) {   // block failures
      for (auto id : failed_ids) {
        failures_map[stripe_id].push_back((int)id);
      }
    } else {   // node failures
      int num_of_failed_nodes = int(failed_ids.size());
      for (int i = 0; i < num_of_failed_nodes; i++) {
        unsigned int node_id = failed_ids[i];
        for (auto it = stripe_table_.begin(); it != stripe_table_.end(); it++) {
          int t_stripe_id = it->first;
          auto& stripe = it->second;
          int n = stripe.ec->k + stripe.ec->m;
          int failed_block_idx = -1;
          for (int j = 0; j < n; j++) {
            if (stripe.blocks2nodes[j] == node_id) {
              failed_block_idx = j;
              break;
            }
          }
          if (failures_map.find(t_stripe_id) != failures_map.end()) {
            failures_map[t_stripe_id].push_back(failed_block_idx);
          } else {
            std::vector<int> failed_blocks;
            failed_blocks.push_back(failed_block_idx);
            failures_map[t_stripe_id] = failed_blocks;
          }
        }
      }
    }
  }

  bool Coordinator::concrete_repair_plans(
          int stripe_id,
          std::vector<RepairPlan>& repair_plans,
          std::vector<MainRepairPlan>& main_repairs,
          std::vector<std::vector<HelpRepairPlan>>& help_repairs)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    for (auto& repair_plan : repair_plans) {
      std::unordered_map<int, unsigned int> map2clusters;
      int cnt = 0;
      for (auto& help_block : repair_plan.help_blocks) {
        unsigned int nid = stripe.blocks2nodes[help_block[0]];
        unsigned int cid = node_table_[nid].map2cluster;
        map2clusters[cnt++] = cid;
      }
      unsigned int main_nid = stripe.blocks2nodes[repair_plan.failure_idxs[0]];
      unsigned int main_cid = node_table_[main_nid].map2cluster;
      // for new locations, to optimize
      std::unordered_map<unsigned int, std::vector<unsigned int>> free_nodes_in_clusters;
      for (auto it = repair_plan.failure_idxs.begin();
           it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = stripe.blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        if (free_nodes_in_clusters.find(cluster_id) ==
            free_nodes_in_clusters.end()) {
          std::vector<unsigned int> free_nodes;
          Cluster &cluster = cluster_table_[cluster_id];
          for (int i = 0; i < num_of_nodes_per_cluster_; i++) {
            free_nodes.push_back(cluster.nodes[i]);
          }
          free_nodes_in_clusters[cluster_id] = free_nodes;
        }
        auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                              free_nodes_in_clusters[cluster_id].end(), node_id);
        free_nodes_in_clusters[cluster_id].erase(iter);
      }

      MainRepairPlan main_plan;
      int clusters_num = repair_plan.help_blocks.size();
      for (int i = 0; i < clusters_num; i++) {
        for (auto block_idx : repair_plan.help_blocks[i]) {
          main_plan.live_blocks_index.push_back(block_idx);
        }
        if (main_cid == map2clusters[i]) {
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = stripe.blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            main_plan.inner_cluster_help_blocks_info.push_back(
                std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
            main_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            // for new locations
            unsigned int cluster_id = node_table_[node_id].map2cluster;
            auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                                  free_nodes_in_clusters[cluster_id].end(), node_id);
            free_nodes_in_clusters[cluster_id].erase(iter);
          }
        }
      }
      main_plan.ec_type = ec_schema_.ec_type;
      stripe.ec->get_coding_parameters(main_plan.cp);
      main_plan.cluster_id = main_cid;
      main_plan.cp.x = ec_schema_.x;
      main_plan.cp.seri_num = stripe_id % ec_schema_.x;
      main_plan.cp.local_or_column = repair_plan.local_or_column;
      main_plan.block_size = stripe.block_size;
      main_plan.partial_decoding = ec_schema_.partial_decoding;
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        main_plan.failed_blocks_index.push_back(*it);
        main_plan.failed_block_ids.push_back(stripe.block_ids[*it]);
      }
      std::vector<HelpRepairPlan> help_plans;
      for (int i = 0; i < clusters_num; i++) {
        if (map2clusters[i] != main_cid) {
          HelpRepairPlan help_plan;
          help_plan.ec_type = main_plan.ec_type;
          help_plan.cp = main_plan.cp;
          help_plan.cluster_id = map2clusters[i];
          help_plan.block_size = main_plan.block_size;
          help_plan.partial_decoding = main_plan.partial_decoding;
          help_plan.isvertical = main_plan.isvertical;
          for(auto it = repair_plan.failure_idxs.begin();
              it != repair_plan.failure_idxs.end(); it++) {
            help_plan.failed_blocks_index.push_back(*it);
          }
          for(auto it = main_plan.live_blocks_index.begin(); 
              it != main_plan.live_blocks_index.end(); it++) {
            help_plan.live_blocks_index.push_back(*it);
          }
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = stripe.blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            help_plan.inner_cluster_help_blocks_info.push_back(
                std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
            help_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
          }
          main_plan.help_clusters_blocks_info.push_back(
              help_plan.inner_cluster_help_blocks_info);
          main_plan.help_clusters_block_ids.push_back(
              help_plan.inner_cluster_help_block_ids);
          help_plan.main_proxy_ip = cluster_table_[main_cid].proxy_ip;
          help_plan.main_proxy_port =
              cluster_table_[main_cid].proxy_port + SOCKET_PORT_OFFSET;
          help_plans.push_back(help_plan);
        }
      }
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
          unsigned int node_id = stripe.blocks2nodes[*it];
          unsigned int cluster_id = node_table_[node_id].map2cluster;
          std::vector<unsigned int> &free_nodes = free_nodes_in_clusters[cluster_id];
          int ran_node_idx = -1;
          unsigned int new_node_id = 0;
          ran_node_idx = random_index(free_nodes.size());
          new_node_id = free_nodes[ran_node_idx];
          auto iter = std::find(free_nodes.begin(), free_nodes.end(), new_node_id);
          free_nodes.erase(iter);
            
          std::string node_ip = node_table_[new_node_id].node_ip;
          int node_port = node_table_[new_node_id].node_port;
          main_plan.new_locations.push_back(
              std::make_pair(cluster_id, std::make_pair(node_ip, node_port)));
        }
        main_repairs.push_back(main_plan);
        help_repairs.push_back(help_plans);
    }
    return true;
  }

  bool Coordinator::concrete_repair_plans_pc(
          int stripe_id,
          std::vector<RepairPlan>& repair_plans,
          std::vector<MainRepairPlan>& main_repairs,
          std::vector<std::vector<HelpRepairPlan>>& help_repairs)
  {
    Stripe& stripe = stripe_table_[stripe_id];
    CodingParameters cp;
    stripe.ec->get_coding_parameters(cp);
    ProductCode pc;
    pc.init_coding_parameters(cp);
    for (auto& repair_plan : repair_plans) {
      std::unordered_map<int, unsigned int> map2clusters;
      int cnt = 0;
      for (auto& help_block : repair_plan.help_blocks) {
        unsigned int nid = stripe.blocks2nodes[help_block[0]];
        unsigned int cid = node_table_[nid].map2cluster;
        map2clusters[cnt++] = cid;
      }
      unsigned int main_nid = stripe.blocks2nodes[repair_plan.failure_idxs[0]];
      unsigned int main_cid = node_table_[main_nid].map2cluster;
      // for new locations, to optimize
      std::unordered_map<unsigned int, std::vector<unsigned int>> free_nodes_in_clusters;
      for (auto it = repair_plan.failure_idxs.begin();
           it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = stripe.blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        if (free_nodes_in_clusters.find(cluster_id) ==
            free_nodes_in_clusters.end()) {
          std::vector<unsigned int> free_nodes;
          Cluster &cluster = cluster_table_[cluster_id];
          for (int i = 0; i < num_of_nodes_per_cluster_; i++) {
            free_nodes.push_back(cluster.nodes[i]);
          }
          free_nodes_in_clusters[cluster_id] = free_nodes;
        }
        auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                              free_nodes_in_clusters[cluster_id].end(), node_id);
        free_nodes_in_clusters[cluster_id].erase(iter);
      }

      int row = -1, col = -1;
      MainRepairPlan main_plan;
      if (ec_schema_.multistripe_placement_rule == VERTICAL) {
          main_plan.isvertical = true;
      }
      int clusters_num = repair_plan.help_blocks.size();
      for (int i = 0; i < clusters_num; i++) {
        for (auto block_idx : repair_plan.help_blocks[i]) {
          pc.bid2rowcol(block_idx, row, col);
          if (repair_plan.local_or_column) {
            main_plan.live_blocks_index.push_back(row);
          } else {
            main_plan.live_blocks_index.push_back(col);
          }
          
        }
        if (main_cid == map2clusters[i]) {
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = stripe.blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            pc.bid2rowcol(block_idx, row, col);
            if (repair_plan.local_or_column) {
              main_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(row, std::make_pair(node_ip, node_port)));
            } else {
              main_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(col, std::make_pair(node_ip, node_port)));
            }
            main_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
            // for new locations
            unsigned int cluster_id = node_table_[node_id].map2cluster;
            auto iter = std::find(free_nodes_in_clusters[cluster_id].begin(), 
                                  free_nodes_in_clusters[cluster_id].end(), node_id);
            free_nodes_in_clusters[cluster_id].erase(iter);
          }
        }
      }
      if (ec_schema_.ec_type == Hierachical_PC) {
        if (repair_plan.local_or_column) {
          main_plan.cp.k = pc.k2;
          main_plan.cp.m = pc.m2;
          if (main_plan.isvertical) {
            main_plan.ec_type = ERS;
          } else {
            main_plan.ec_type = RS;
          }
        } else {
          main_plan.cp.k = pc.k1;
          main_plan.cp.m = pc.m1;
          if (main_plan.isvertical) {
            main_plan.ec_type = RS;
          } else {
            main_plan.ec_type = ERS;
          }
        }
      } else {
        main_plan.ec_type = RS;
        if (repair_plan.local_or_column) {
          main_plan.cp.k = pc.k2;
          main_plan.cp.m = pc.m2;
        } else {
          main_plan.cp.k = pc.k1;
          main_plan.cp.m = pc.m1;
        }
      }
      main_plan.cluster_id = main_cid;
      main_plan.cp.x = ec_schema_.x;
      main_plan.cp.seri_num = stripe_id % ec_schema_.x;
      main_plan.cp.local_or_column = repair_plan.local_or_column;
      main_plan.block_size = stripe.block_size;
      main_plan.partial_decoding = ec_schema_.partial_decoding;
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        pc.bid2rowcol(*it, row, col);
        if (repair_plan.local_or_column) {
          main_plan.failed_blocks_index.push_back(row);
        } else {
          main_plan.failed_blocks_index.push_back(col);
        }
        main_plan.failed_block_ids.push_back(stripe.block_ids[*it]);
      }
      std::vector<HelpRepairPlan> help_plans;
      for (int i = 0; i < clusters_num; i++) {
        if (map2clusters[i] != main_cid) {
          HelpRepairPlan help_plan;
          help_plan.ec_type = main_plan.ec_type;
          help_plan.cp = main_plan.cp;
          help_plan.cluster_id = map2clusters[i];
          help_plan.block_size = main_plan.block_size;
          help_plan.partial_decoding = main_plan.partial_decoding;
          help_plan.isvertical = main_plan.isvertical;
          for(auto it = main_plan.failed_blocks_index.begin();
              it != main_plan.failed_blocks_index.end(); it++) {
            help_plan.failed_blocks_index.push_back(*it);
          }
          for(auto it = main_plan.live_blocks_index.begin(); 
              it != main_plan.live_blocks_index.end(); it++) {
            help_plan.live_blocks_index.push_back(*it);
          }
          for (auto block_idx : repair_plan.help_blocks[i]) {
            unsigned int node_id = stripe.blocks2nodes[block_idx];
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            pc.bid2rowcol(block_idx, row, col);
            if (repair_plan.local_or_column) {
              main_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(row, std::make_pair(node_ip, node_port)));
            } else {
              main_plan.inner_cluster_help_blocks_info.push_back(
                  std::make_pair(col, std::make_pair(node_ip, node_port)));
            }
            help_plan.inner_cluster_help_block_ids.push_back(
                stripe.block_ids[block_idx]);
          }
          main_plan.help_clusters_blocks_info.push_back(
              help_plan.inner_cluster_help_blocks_info);
          main_plan.help_clusters_block_ids.push_back(
              help_plan.inner_cluster_help_block_ids);
          help_plan.main_proxy_ip = cluster_table_[main_cid].proxy_ip;
          help_plan.main_proxy_port =
              cluster_table_[main_cid].proxy_port + SOCKET_PORT_OFFSET;
          help_plans.push_back(help_plan);
        }
      }
      for(auto it = repair_plan.failure_idxs.begin();
          it != repair_plan.failure_idxs.end(); it++) {
        unsigned int node_id = stripe.blocks2nodes[*it];
        unsigned int cluster_id = node_table_[node_id].map2cluster;
        std::vector<unsigned int> &free_nodes = free_nodes_in_clusters[cluster_id];
        int ran_node_idx = -1;
        unsigned int new_node_id = 0;
        ran_node_idx = random_index(free_nodes.size());
        new_node_id = free_nodes[ran_node_idx];
        auto iter = std::find(free_nodes.begin(), free_nodes.end(), new_node_id);
        free_nodes.erase(iter);
            
        std::string node_ip = node_table_[new_node_id].node_ip;
        int node_port = node_table_[new_node_id].node_port;
        main_plan.new_locations.push_back(
            std::make_pair(cluster_id, std::make_pair(node_ip, node_port)));
      }
      main_repairs.push_back(main_plan);
      help_repairs.push_back(help_plans);
    }
    return true;
  }

  void Coordinator::simulation_repair(
          std::vector<MainRepairPlan>& main_repair,
          int& cross_cluster_transfers)
  {
    for (int i = 0; i < int(main_repair.size()); i++) {
      int failed_num = int(main_repair[i].failed_blocks_index.size());
      for (int j = 0; j < int(main_repair[i].help_clusters_blocks_info.size()); j++) {
        int num_of_help_blocks = int(main_repair[i].help_clusters_blocks_info[j].size());
        if (num_of_help_blocks > failed_num && ec_schema_.partial_decoding) {
          cross_cluster_transfers += failed_num;
        } else {
          cross_cluster_transfers += num_of_help_blocks;
        }
      }
    }
  }
}