#pragma once

#include "ec/utils.h"
#include "ec/erasure_code.h"
#include "ec/rs.h"
#include "ec/lrc.h"
#include "ec/pc.h"

#define LOG_TO_FILE true
#define IF_SIMULATION false
#define IF_SIMULATE_CROSS_CLUSTER false
#define IF_TEST_TRHROUGHPUT false
#define IF_DEBUG true
#define IF_DIRECT_FROM_NODE true    // proxy can directly access data from nodes in other clusters
#define SOCKET_PORT_OFFSET 500
#define STORAGE_SERVER_OFFSET 1000
#define IN_MEMORY true
// #define MEMCACHED true
// #define REDIS true
#define COORDINATOR_PORT 12121
#define CLIENT_PORT 21212

namespace ECProject
{
  enum MultiStripePR
  {
    RAND,
    DISPERSED,
    AGGREGATED,
    HORIZONTAL,
    VERTICAL
  };

  struct ECSchema
  {
    bool partial_decoding;
    ECTYPE ec_type;
    ErasureCode *ec = nullptr;
    PlacementRule placement_rule;
    MultiStripePR multistripe_placement_rule;
    int x;
    size_t block_size;  // bytes

    ~ECSchema()
    {
      if (ec!= nullptr) {
        delete ec;
      }
    }

    void set_ec(ErasureCode *new_ec) {
      if (ec != nullptr) {
        delete ec;
        ec = nullptr;
      }
      ec = new_ec;
    }
  };

  struct ObjectInfo
  {
    size_t value_len;
    std::vector<unsigned int> stripes;
  };
  
  struct Stripe
  {
    unsigned int stripe_id;
    ErasureCode *ec = nullptr;
    size_t block_size;
    // data blocks, local parity blocks, global parity blocks in order
    std::vector<unsigned int> blocks2nodes;
    std::vector<unsigned int> block_ids;
    std::vector<std::string> objects;   // in order with data blocks

    ~Stripe()
    {
      if (ec != nullptr) {
        delete ec;
        ec = nullptr;
      }
    }
  };
  
  struct Node
  {
    unsigned int node_id;
    std::string node_ip;
    int node_port;
    unsigned int map2cluster;
  };

  struct Cluster
  {
    unsigned int cluster_id;
    std::string proxy_ip;
    int proxy_port;
    std::vector<unsigned int> nodes;
    std::unordered_set<unsigned int> holding_stripe_ids;  
  };
  
  struct ParametersInfo
  {
    bool partial_decoding;
    ECTYPE ec_type;
    PlacementRule placement_rule;
    MultiStripePR multistripe_placement_rule;
    CodingParameters cp;
    size_t block_size;
  };
  
  struct PlacementInfo
  {
    ECTYPE ec_type;
    CodingParameters cp;
    std::vector<unsigned int> stripe_ids;
    std::vector<int> offsets; // for cross-object striping
    std::vector<int> seri_nums;
    bool isvertical = false;
    bool merged_flag = false;
    std::string key;
    size_t value_len;
    size_t block_size;
    size_t tail_block_size;
    std::vector<unsigned int> block_ids;
    std::vector<std::pair<unsigned int, std::pair<std::string, int>>> datanode_ip_port;
    std::string client_ip;
    int client_port;
  };

  struct DeletePlan
  {
    std::vector<unsigned int> block_ids;
    std::vector<std::pair<std::string, int>> blocks_info;
  };
    
  struct MainRepairPlan
  {
    ECTYPE ec_type;
    CodingParameters cp;
    unsigned int cluster_id;
    size_t block_size;
    bool partial_decoding;
    bool isvertical = false;
    std::vector<int> live_blocks_index;
    std::vector<int> failed_blocks_index;
    std::vector<std::vector<std::pair<int, std::pair<std::string, int>>>> 
        help_clusters_blocks_info;
     std::vector<std::vector<unsigned int>> help_clusters_block_ids;
    std::vector<std::pair<int, std::pair<std::string, int>>> 
        inner_cluster_help_blocks_info;
    std::vector<unsigned int> inner_cluster_help_block_ids;
    std::vector<std::pair<unsigned int, std::pair<std::string, int>>> new_locations;
    std::vector<unsigned int> failed_block_ids;
  };

  struct HelpRepairPlan
  {
    ECTYPE ec_type;
    CodingParameters cp;
    unsigned int cluster_id;
    size_t block_size;
    bool partial_decoding;
    bool isvertical = false;
    std::vector<int> live_blocks_index;
    // the order of blocks index must be consistent with that in main_repair_plan
    std::vector<int> failed_blocks_index;   
    std::vector<std::pair<int, std::pair<std::string, int>>> 
        inner_cluster_help_blocks_info;
    std::vector<unsigned int> inner_cluster_help_block_ids;
    std::string main_proxy_ip;
    int main_proxy_port;
  };

  struct LocationInfo
  {
    unsigned int cluster_id;
    int proxy_port;
    std::string proxy_ip;
    std::vector<std::pair<int, std::pair<std::string, int>>> blocks_info;
    std::vector<unsigned int> block_ids;
  };
  
  struct MainRecalPlan
  {
    ECTYPE ec_type;
    CodingParameters cp;
    unsigned int cluster_id;
    size_t block_size;
    bool partial_decoding;
    bool isvertical = false;
    std::vector<LocationInfo> help_clusters_info;
    std::vector<std::pair<int, std::pair<std::string, int>>> 
        inner_cluster_help_blocks_info;
    std::vector<unsigned int> inner_cluster_help_block_ids;
    std::vector<std::pair<int, std::pair<std::string, int>>> new_locations;
    std::vector<unsigned int> new_parity_block_ids;
  };

  struct HelpRecalPlan
  {
    ECTYPE ec_type;
    CodingParameters cp;
    size_t block_size;
    int main_proxy_port;
    std::string main_proxy_ip;
    bool partial_decoding;
    bool isvertical = false;
    std::vector<std::pair<int, std::pair<std::string, int>>> 
        inner_cluster_help_blocks_info;
    std::vector<unsigned int> inner_cluster_help_block_ids;
    std::vector<int> new_parity_block_idxs;
  };

  struct RelocatePlan
  {
    size_t block_size;
    std::vector<unsigned int> blocks_to_move;
    std::vector<std::pair<unsigned int, std::pair<std::string, int>>> src_nodes;
    std::vector<std::pair<unsigned int, std::pair<std::string, int>>> des_nodes;
  };
  
  // response
  struct SetResp
  {
    std::string proxy_ip;
    int proxy_port;
  };

  struct RepairResp
  {
    double decoding_time;
    double cross_cluster_time;
    double repair_time;
    double meta_time;
    int cross_cluster_transfers;
  };

  struct MergeResp
  {
    double computing_time;
    double cross_cluster_time;
    double merging_time;
    double meta_time;
    int cross_cluster_transfers;
  };

  struct MainRepairResp
  {
    double decoding_time;
    double cross_cluster_time;
  };

  struct MainRecalResp
  {
    double computing_time;
    double cross_cluster_time;
  };

  struct RelocateResp
  {
    double cross_cluster_time;
  };
  
  ECFAMILY check_ec_family(ECTYPE ec_type);
  ErasureCode* ec_factory(ECTYPE ec_type, CodingParameters cp);
  RSCode* rs_factory(ECTYPE ec_type, CodingParameters cp);
  LocallyRepairableCode* lrc_factory(ECTYPE ec_type, CodingParameters cp);
  ProductCode* pc_factory(ECTYPE ec_type, CodingParameters cp);
  ErasureCode* clone_ec(ECTYPE ec_type, ErasureCode* ec);
  void parse_args(ParametersInfo& paras, std::string config_file);
  int stripe_wide_after_merge(ParametersInfo paras, int step_size);
}