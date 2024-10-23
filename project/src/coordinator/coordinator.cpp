#include "coordinator.h"

namespace ECProject
{
  Coordinator::Coordinator(std::string ip, int port, std::string xml_path)
      : ip_(ip), port_(port), xml_path_(xml_path)
  {
    easylog::set_min_severity(easylog::Severity::ERROR);
    rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(4, port_);
    rpc_server_->register_handler<&Coordinator::checkalive>(this);
    rpc_server_->register_handler<&Coordinator::set_erasure_coding_parameters>(this);
    rpc_server_->register_handler<&Coordinator::request_set>(this);
    rpc_server_->register_handler<&Coordinator::commit_object>(this);
    rpc_server_->register_handler<&Coordinator::request_get>(this);
    rpc_server_->register_handler<&Coordinator::request_delete_by_stripe>(this);
    rpc_server_->register_handler<&Coordinator::request_repair>(this);
    rpc_server_->register_handler<&Coordinator::request_merge>(this);
    rpc_server_->register_handler<&Coordinator::list_stripes>(this);

    cur_stripe_id_ = 0;
    cur_block_id_ = 0;
    time_ = 0;
    
    init_cluster_info();
    init_proxy_info();
    if (IF_DEBUG) {
      std::cout << "Start the coordinator! " << ip << ":" << port << std::endl;
    }
  }
  Coordinator::~Coordinator() { rpc_server_->stop(); }

  void Coordinator::run() { auto err = rpc_server_->start(); }

  std::string Coordinator::checkalive(std::string msg) 
  { 
    return msg + " Hello, it's me. The coordinator!"; 
  }

  void Coordinator::set_erasure_coding_parameters(ParametersInfo paras)
  {
    ec_schema_.partial_decoding = paras.partial_decoding;
    ec_schema_.ec_type = paras.ec_type;
    ec_schema_.placement_rule = paras.placement_rule;
    ec_schema_.multistripe_placement_rule = paras.multistripe_placement_rule;
    ec_schema_.x = paras.cp.x;
    ec_schema_.object_size_upper = paras.object_size_upper;
    ec_schema_.ec = ec_factory(paras.ec_type, paras.cp);
    reset_metadata();
  }

  SetResp Coordinator::request_set(
              std::vector<std::pair<std::string, size_t>> objects)
  {
    int num_of_objects = (int)objects.size();
    my_assert(num_of_objects > 0);
    mutex_.lock();
    for (int i = 0; i < num_of_objects; i++) {
      std::string key = objects[i].first;
      if (commited_object_table_.contains(key)) {
        mutex_.unlock();
        my_assert(false);
      }
    }
    mutex_.unlock();

    size_t total_value_len = 0;
    std::vector<ObjectInfo> new_objects;
    for (int i = 0; i < num_of_objects; i++) {
      // currently only support small objects with regular size, for cross-object striping
      ObjectInfo new_object;
      new_object.value_len = objects[i].second;
      total_value_len += new_object.value_len;
      new_objects.push_back(new_object);
    }
    

    if (IF_DEBUG) {
      std::cout << "[SET] Ready to process " << num_of_objects
                << " objects. Each with length of "
                << total_value_len / num_of_objects << std::endl;
    }

    PlacementInfo placement;

    if (ec_schema_.object_size_upper >= total_value_len) {
      size_t block_size = 
        std::ceil(static_cast<double>(total_value_len) / 
                  static_cast<double>(ec_schema_.ec->k));
      block_size = 64 * std::ceil(static_cast<double>(block_size) / 64.0);
      init_placement_info(placement, objects[0].first,
                          total_value_len, block_size, 0);

      Stripe& stripe = new_stripe(block_size);
      for (int i = 0; i < num_of_objects; i++) {
        stripe.objects.push_back(objects[i].first);
        new_objects[i].stripes.push_back(stripe.stripe_id);
      }

      if (IF_DEBUG) {
        std::cout << "[SET] Ready to data placement for stripe "
                  << stripe.stripe_id << std::endl;
      }

      generate_placement(stripe.stripe_id);

      if (IF_DEBUG) {
        std::cout << "[SET] Finish data placement for stripe "
                  << stripe.stripe_id << std::endl;
      }

      placement.stripe_ids.push_back(stripe.stripe_id);
      placement.seri_nums.push_back(stripe.stripe_id % ec_schema_.x);
      for (auto& node_id : stripe.blocks2nodes) {
        auto& node = node_table_[node_id];
        placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
            std::make_pair(node.node_ip, node.node_port)));
      }
      for (auto& block_id : stripe.block_ids) {
        placement.block_ids.push_back(block_id);
      }
    } else {
      size_t block_size = 
        std::ceil(static_cast<double>(ec_schema_.object_size_upper) / 
                  static_cast<double>(ec_schema_.ec->k));
      block_size = 64 * std::ceil(static_cast<double>(block_size) / 64.0);
      init_placement_info(placement, objects[0].first,
                          total_value_len, block_size, -1);
            
      int num_of_stripes = total_value_len / (ec_schema_.ec->k * block_size);
      size_t left_value_len = total_value_len;
      int cumulative_len = 0, obj_idx = 0;
      for (int i = 0; i < num_of_stripes; i++) {
        left_value_len -= ec_schema_.ec->k * block_size;
        auto &stripe = new_stripe(block_size);
        while (cumulative_len < ec_schema_.ec->k * block_size) {
          stripe.objects.push_back(objects[obj_idx].first);
          new_objects[obj_idx].stripes.push_back(stripe.stripe_id);
          cumulative_len += objects[obj_idx].second;
          obj_idx++;
        }

        if (IF_DEBUG) {
          std::cout << "[SET] Ready to data placement for stripe "
                    << stripe.stripe_id << std::endl;
        }

        generate_placement(stripe.stripe_id);

        if (IF_DEBUG) {
          std::cout << "[SET] Finish data placement for stripe "
                    << stripe.stripe_id << std::endl;
        }

        placement.stripe_ids.push_back(stripe.stripe_id);
        placement.seri_nums.push_back(stripe.stripe_id % ec_schema_.x);
        for (auto& node_id : stripe.blocks2nodes) {
          auto& node = node_table_[node_id];
          placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
            std::make_pair(node.node_ip, node.node_port)));
        }
        for (auto& block_id : stripe.block_ids) {
          placement.block_ids.push_back(block_id);
        }
      }
      if (left_value_len > 0) {
        size_t tail_block_size = std::ceil(static_cast<double>(left_value_len) /
                                           static_cast<double>(ec_schema_.ec->k));
        tail_block_size = 64 * std::ceil(static_cast<double>(tail_block_size) / 64.0);
        placement.tail_block_size = tail_block_size;
        auto &stripe = new_stripe(tail_block_size);
        if (cumulative_len > ec_schema_.ec->k * block_size) {// for object cross stripe
          stripe.objects.push_back(objects[obj_idx - 1].first);
          new_objects[obj_idx - 1].stripes.push_back(stripe.stripe_id);
        }
        while (obj_idx < num_of_objects) {
          stripe.objects.push_back(objects[obj_idx].first);
          new_objects[obj_idx].stripes.push_back(stripe.stripe_id);
          cumulative_len += objects[obj_idx].second;
          obj_idx++;
        }
        my_assert(cumulative_len == total_value_len);

        if (IF_DEBUG) {
          std::cout << "[SET] Ready to data placement for stripe "
                    << stripe.stripe_id << std::endl;
        }

        generate_placement(stripe.stripe_id);

        if (IF_DEBUG) {
          std::cout << "[SET] Finish data placement for stripe "
                    << stripe.stripe_id << std::endl;
        }
                
        placement.stripe_ids.push_back(stripe.stripe_id);
        placement.seri_nums.push_back(stripe.stripe_id % ec_schema_.x);
        for (auto& node_id : stripe.blocks2nodes) {
          auto& node = node_table_[node_id];
          placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
            std::make_pair(node.node_ip, node.node_port)));
        }
        for (auto& block_id : stripe.block_ids) {
          placement.block_ids.push_back(block_id);
        }
      }
    }

    mutex_.lock();
    for (int i = 0; i < num_of_objects; i++) {
      updating_object_table_[objects[i].first] = new_objects[i];
    }
    mutex_.unlock();

    unsigned int node_id = stripe_table_[cur_stripe_id_ - 1].blocks2nodes[0];
    unsigned int selected_cluster_id = node_table_[node_id].map2cluster;
    std::string selected_proxy_ip = cluster_table_[selected_cluster_id].proxy_ip;
    int selected_proxy_port = cluster_table_[selected_cluster_id].proxy_port;

    if (IF_DEBUG) {
      std::cout << "[SET] Select proxy " << selected_proxy_ip << ":" 
                << selected_proxy_port << " in cluster "
                << selected_cluster_id << " to handle encode and set!" << std::endl;
    }

    if (IF_SIMULATION) {// simulation, commit object
      mutex_.lock();
      for (int i = 0; i < num_of_objects; i++) {
        my_assert(commited_object_table_.contains(objects[i].first) == false &&
                  updating_object_table_.contains(objects[i].first) == true);
        commited_object_table_[objects[i].first] = updating_object_table_[objects[i].first];
        updating_object_table_.erase(objects[i].first);
      }
      mutex_.unlock();
    } else {
      async_simple::coro::syncAwait(
        proxies_[selected_proxy_ip + std::to_string(selected_proxy_port)]
            ->call<&Proxy::encode_and_store_object>(placement));
    }

    SetResp response;
    response.proxy_ip = selected_proxy_ip;
    response.proxy_port = selected_proxy_port + SOCKET_PORT_OFFSET; // port for transfer data

    return response;
  }

  void Coordinator::commit_object(std::vector<std::string> keys, bool commit)
  {
    int num = (int)keys.size();
    if (commit) {
      mutex_.lock();
      for (int i = 0; i < num; i++) {
        my_assert(commited_object_table_.contains(keys[i]) == false &&
                  updating_object_table_.contains(keys[i]) == true);
        commited_object_table_[keys[i]] = updating_object_table_[keys[i]];
        updating_object_table_.erase(keys[i]);
      }
      mutex_.unlock();
    } else {
      for (int i = 0; i < num; i++) {
        ObjectInfo& objects = updating_object_table_[keys[i]];
        for (auto stripe_id : objects.stripes) {
          for (auto it = cluster_table_.begin(); it != cluster_table_.end(); it++) {
            Cluster& cluster = it->second;
            cluster.holding_stripe_ids.erase(stripe_id);
          }
          stripe_table_.erase(stripe_id);
          cur_stripe_id_--;
        }
      }
      mutex_.lock();
      for (int i = 0; i < num; i++) {
        my_assert(commited_object_table_.contains(keys[i]) == false &&
                  updating_object_table_.contains(keys[i]) == true);
        updating_object_table_.erase(keys[i]);
      }
      mutex_.unlock();
    }
  }

  size_t Coordinator::request_get(std::string key, std::string client_ip,
                                  int client_port)
  {
    mutex_.lock();
    if (commited_object_table_.contains(key) == false) {
      mutex_.unlock();
      my_assert(false);
    }
    ObjectInfo &object = commited_object_table_[key];
    mutex_.unlock();

    PlacementInfo placement;
    if (ec_schema_.object_size_upper >= object.value_len) {
      size_t block_size = std::ceil(static_cast<double>(object.value_len) /
                                  static_cast<double>(ec_schema_.ec->k));
      block_size = 64 * std::ceil(static_cast<double>(block_size) / 64.0);
      init_placement_info(placement, key, object.value_len, block_size, 0);
    } else {
      size_t block_size = std::ceil(static_cast<double>(ec_schema_.object_size_upper) /
                                  static_cast<double>(ec_schema_.ec->k));
      block_size = 64 * std::ceil(static_cast<double>(block_size) / 64.0);
      size_t tail_block_size = -1;
      if (object.value_len % (ec_schema_.ec->k * block_size) != 0) {
        size_t tail_stripe_size = object.value_len % (ec_schema_.ec->k * block_size);
        tail_block_size = 
            std::ceil(static_cast<double>(tail_stripe_size) /
                      static_cast<double>(ec_schema_.ec->k));
        tail_block_size = 64 * std::ceil(static_cast<double>(tail_block_size) / 64.0);
      }
      init_placement_info(placement, key, object.value_len,
                          block_size, tail_block_size);
    }

    for (auto stripe_id : object.stripes) {
      Stripe &stripe = stripe_table_[stripe_id];
      placement.stripe_ids.push_back(stripe_id);

      int num_of_object_in_a_stripe = (int)stripe.objects.size();
      int offset = 0;
      for (int i = 0; i < num_of_object_in_a_stripe; i++) {
        if (stripe.objects[i] != key) {
          int t_object_len = commited_object_table_[stripe.objects[i]].value_len;
          offset += t_object_len / stripe.block_size;     // must be block_size of stripe
        } else {
          break;
        }
      }
      placement.offsets.push_back(offset);

      for (auto& node_id : stripe.blocks2nodes) {
        auto& node = node_table_[node_id];
        placement.datanode_ip_port.push_back(std::make_pair(node.map2cluster,
            std::make_pair(node.node_ip, node.node_port)));
      }
      for (auto& block_id : stripe.block_ids) {
        placement.block_ids.push_back(block_id);
      }
    }

    if (!IF_SIMULATION) {
      placement.client_ip = client_ip;
      placement.client_port = client_port;
      int selected_proxy_id = random_index(cluster_table_.size());
      std::string location = cluster_table_[selected_proxy_id].proxy_ip +
          std::to_string(cluster_table_[selected_proxy_id].proxy_port);
      if (IF_DEBUG) {
        std::cout << "[GET] Select proxy "
                  << cluster_table_[selected_proxy_id].proxy_ip << ":" 
                  << cluster_table_[selected_proxy_id].proxy_port
                  << " in cluster "
                  << cluster_table_[selected_proxy_id].cluster_id
                  << " to handle get!" << std::endl;
      }
      async_simple::coro::syncAwait(
          proxies_[location]->call<&Proxy::decode_and_get_object>(placement));
    }

    return object.value_len;
  }

  void Coordinator::request_delete_by_stripe(std::vector<unsigned int> stripe_ids)
  {
    std::unordered_set<std::string> objects_key;
    int num_of_stripes = (int)stripe_ids.size();
    if (IF_DEBUG) {
        std::cout << "[DELETE] Delete " << num_of_stripes << " stripes!\n";
    }
    DeletePlan delete_info;
    for (int i = 0; i < num_of_stripes; i++) {
      auto &stripe = stripe_table_[stripe_ids[i]];
      for (auto key : stripe.objects) {
        objects_key.insert(key);
      }
      int n = stripe.ec->k + stripe.ec->m;
      my_assert(n == (int)stripe.blocks2nodes.size());
      for (int j = 0; j < n; j++) {
        delete_info.block_ids.push_back(stripe.block_ids[j]);
        auto &node = node_table_[stripe.blocks2nodes[j]];
        delete_info.blocks_info.push_back({node.node_ip, node.node_port});
      }
    }

    if (!IF_SIMULATION) {
      int selected_proxy_id = random_index(cluster_table_.size());
      std::string location = cluster_table_[selected_proxy_id].proxy_ip +
          std::to_string(cluster_table_[selected_proxy_id].proxy_port);
      async_simple::coro::syncAwait(
          proxies_[location]->call<&Proxy::delete_blocks>(delete_info));
    }

    // update meta data
    for (int i = 0; i < num_of_stripes; i++) {
      for (auto it = cluster_table_.begin(); it != cluster_table_.end(); it++) {
        Cluster& cluster = it->second;
        cluster.holding_stripe_ids.erase(stripe_ids[i]);
      }
      stripe_table_.erase(stripe_ids[i]);
    }
    mutex_.lock();
    for (auto key : objects_key) {
      commited_object_table_.erase(key);
    }
    mutex_.unlock();
    if (IF_DEBUG) {
      std::cout << "[DELETE] Finish delete!" << std::endl;
    }
  }

  std::vector<unsigned int> Coordinator::list_stripes()
  {
    std::vector<unsigned int> stripe_ids;
    for (auto it = stripe_table_.begin(); it != stripe_table_.end(); it++) {
      stripe_ids.push_back(it->first);
    }
    return stripe_ids;
  }

  RepairResp Coordinator::request_repair(std::vector<unsigned int> failed_ids, int stripe_id)
  {
    RepairResp response;
    do_repair(failed_ids, stripe_id, response);
    return response;
  }

  MergeResp Coordinator::request_merge(int step_size)
  {
    MergeResp response;
    do_stripe_merge(response, step_size);
    return response;
  }
}