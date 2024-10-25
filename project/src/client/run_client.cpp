#include "client.h"
#include <unistd.h>

using namespace ECProject;

void test_single_block_repair(Client &client, int block_num)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::cout << "Single-Block Repair:" << std::endl;
  for (int i = 0; i < stripe_num; i++) {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    for (int j = 0; j < block_num; j++) {
      std::vector<unsigned int> failures;
      failures.push_back((unsigned int)j);
      auto resp = client.blocks_repair(failures, stripe_ids[i]);
      temp_repair += resp.repair_time;
      temp_decoding += resp.decoding_time;
      temp_cross_cluster += resp.cross_cluster_time;
      temp_meta += resp.meta_time;
      temp_cc_transfers += resp.cross_cluster_transfers;
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    std::cout << "repair = " << temp_repair / block_num
              << "s, decoding = " << temp_decoding / block_num
              << "s, cross-cluster = " << temp_cross_cluster / block_num
              << "s, meta = " << temp_meta / block_num
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / block_num
              << std::endl;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
      repair_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_decoding = std::accumulate(decoding_times.begin(),
      decoding_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
      cross_cluster_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_meta = std::accumulate(meta_times.begin(),
      meta_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
      cross_cluster_transfers.end(), 0) / (stripe_num * block_num);
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers << std::endl;
}

void test_multiple_blocks_repair(Client &client, int block_num)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  int run_time = 5;
  std::cout << "Multi-Block Repair:" << std::endl;
  for (int i = 0; i < stripe_num; i++) {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    for (int j = 0; j < run_time; j++) {
      int failed_num = random_range(2, 4);
      std::vector<int> failed_blocks;
      random_n_num(0, block_num - 1, failed_num, failed_blocks);
      std::vector<unsigned int> failures;
      for (auto& block : failed_blocks) {
        failures.push_back((unsigned int)block);
      }
      auto resp = client.blocks_repair(failures, stripe_ids[i]);
      temp_repair += resp.repair_time;
      temp_decoding += resp.decoding_time;
      temp_cross_cluster += resp.cross_cluster_time;
      temp_meta += resp.meta_time;
      temp_cc_transfers += resp.cross_cluster_transfers;
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    std::cout << "repair = " << temp_repair / block_num
              << "s, decoding = " << temp_decoding / block_num
              << "s, cross-cluster = " << temp_cross_cluster / block_num
              << "s, meta = " << temp_meta / block_num
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / block_num
              << std::endl;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
      repair_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_decoding = std::accumulate(decoding_times.begin(),
      decoding_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
      cross_cluster_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_meta = std::accumulate(meta_times.begin(),
      meta_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
      cross_cluster_transfers.end(), 0) / (stripe_num * block_num);
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers << std::endl;
}

void test_stripe_merging(Client &client, int step_size)
{
  my_assert(step_size > 1);
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::cout << "Stripe Merging:" << std::endl;
  auto resp = client.merge(step_size);
  std::cout << "[Total]" << std::endl;
  std::cout << "merging = " << resp.merging_time
            << "s, computing = " << resp.computing_time
            << "s, cross-cluster = " << resp.cross_cluster_time
            << "s, meta =" << resp.meta_time
            << "s, cross-cluster-count = " << resp.cross_cluster_transfers
            << std::endl;
  std::cout << "[Average for every " << step_size << " stripes]" << std::endl;
  std::cout << "merging = " << resp.merging_time / stripe_num
            << "s, computing = " << resp.computing_time / stripe_num
            << "s, cross-cluster = " << resp.cross_cluster_time / stripe_num
            << "s, meta =" << resp.meta_time / stripe_num
            << "s, cross-cluster-count = "
            << (double)resp.cross_cluster_transfers / stripe_num << std::endl;
}

int main(int argc, char **argv)
{
  if (argc != 3) {
    std::cout << "./run_client stripe_num config_file" << std::endl;
    exit(0);
  }

  char buff[256];
  getcwd(buff, 256);
  std::string cwf = std::string(argv[0]);
  std::string config_path = std::string(buff) +
      cwf.substr(1, cwf.rfind('/') - 1) + "/../" + std::string(argv[2]);

  ParametersInfo paras;
  parse_args(paras, config_path);
  int block_num = paras.cp.k + paras.cp.m;

  int stripe_num = std::stoi(argv[1]);
  Client client("0.0.0.0", CLIENT_PORT, "0.0.0.0", COORDINATOR_PORT);

  // set erasure coding parameters
  client.set_ec_parameters(paras);

  struct timeval start_time, end_time;
  // generate key-value pair
  int value_length = (int)paras.block_size * paras.cp.k;
  std::unordered_map<std::string, std::string> key_value;
  generate_unique_random_strings(5, value_length, stripe_num, key_value);

  // set
  double set_time = 0;
  for (auto& kv : key_value) {
    gettimeofday(&start_time, NULL);
    double encoding_time = client.set(kv.first, kv.second);
    gettimeofday(&end_time, NULL);
    double temp_time = end_time.tv_sec - start_time.tv_sec +
        (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
    set_time += temp_time;
    std::cout << "[SET] set time: " << temp_time << ", encoding time: "
              << encoding_time << std::endl;
  }
  std::cout << "Total set time: " << set_time << ", average set time:"
            << set_time / stripe_num << std::endl;

  // single-block repair
  std::cout << "[Pre-merging] ";
  test_single_block_repair(client, block_num);

  // multiple-block repair
  test_multiple_blocks_repair(client, block_num);

  // merge
  test_stripe_merging(client, paras.cp.x);

  // repair
  std::cout << "[Post-merging] ";
  block_num = stripe_wide_after_merge(paras, paras.cp.x);
  test_single_block_repair(client, block_num);

  // multiple-block repair
  test_multiple_blocks_repair(client, block_num);

  // get
  double get_time = 0;
  gettimeofday(&start_time, NULL);
  for (auto &kv : key_value) {
    auto stored_value = client.get(kv.first);
    my_assert(stored_value == kv.second);
  }
  gettimeofday(&end_time, NULL);
  get_time += end_time.tv_sec - start_time.tv_sec +
      (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
  std::cout << "Total get time: " << get_time << ", average get time:"
            << get_time / stripe_num << std::endl;

  // delete
  client.delete_all_stripes();

  return 0;
}