#include "lrc.h"

using namespace ECProject;

void LocallyRepairableCode::init_coding_parameters(CodingParameters cp)
{
  k = cp.k;
  l = cp.l;
  g = cp.g;
  m = l + g;
  local_or_column = cp.local_or_column;
}

void LocallyRepairableCode::get_coding_parameters(CodingParameters& cp)
{
  cp.k = k;
  cp.l = l;
  cp.g = g;
  cp.m = m;
  cp.local_or_column = local_or_column;
}

void LocallyRepairableCode::encode(char **data_ptrs, char **coding_ptrs,
                                   int block_size)
{
  std::vector<int> lrc_matrix((g + l) * k, 0);
  make_encoding_matrix(lrc_matrix.data());
  jerasure_matrix_encode(k, g + l, w, lrc_matrix.data(), data_ptrs,
                         coding_ptrs, block_size);
}

void LocallyRepairableCode::decode(char **data_ptrs, char **coding_ptrs,
                                   int block_size, int *erasures, int failed_num)
{
  if (local_or_column) {
    int group_id = erasures[failed_num];
    erasures[failed_num] = -1;
    decode_local(data_ptrs, coding_ptrs, block_size, erasures, failed_num, group_id);
  } else {
    decode_global(data_ptrs, coding_ptrs, block_size, erasures, failed_num);
  }
}

void LocallyRepairableCode::decode_global(char **data_ptrs, char **coding_ptrs,
        int block_size, int *erasures, int failed_num)
{
  std::vector<int> lrc_matrix((g + l) * k, 0);
  make_encoding_matrix(lrc_matrix.data());
  int ret = 0;
  ret = jerasure_matrix_decode(k, g + l, w, lrc_matrix.data(), failed_num,
                               erasures, data_ptrs, coding_ptrs, block_size);
  if (ret == -1) {
    std::cout << "[Decode] Failed!" << std::endl;
    return;
  }
}

void LocallyRepairableCode::decode_local(char **data_ptrs, char **coding_ptrs,
        int block_size, int *erasures, int failed_num, int group_id)
{
  int min_idx = 0;
  int group_size = get_group_size(group_id, min_idx);
  std::vector<int> group_matrix(1 * group_size, 0);
  make_group_matrix(group_matrix.data(), group_id);
  int ret = 0;
  ret = jerasure_matrix_decode(group_size, 1, w, group_matrix.data(), failed_num,
                               erasures, data_ptrs, coding_ptrs, block_size);
  if (ret == -1) {
    std::cout << "[Decode] Failed!" << std::endl;
    return;
  }
}

void LocallyRepairableCode::encode_partial_blocks_for_encoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs)
{
  if (local_or_column) {
    encode_partial_blocks_for_encoding_local(data_ptrs, coding_ptrs, block_size,
                                             data_idxs, parity_idxs);
  } else {
    encode_partial_blocks_for_encoding_global(data_ptrs, coding_ptrs, block_size,
                                              data_idxs, parity_idxs);
  }
}

void LocallyRepairableCode::encode_partial_blocks_for_decoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs)
{
  if (local_or_column) {
    encode_partial_blocks_for_decoding_local(data_ptrs, coding_ptrs, block_size,
                                             local_survivor_idxs, survivor_idxs,
                                             failure_idxs);
  } else {
    encode_partial_blocks_for_decoding_global(data_ptrs, coding_ptrs, block_size,
                                              local_survivor_idxs, survivor_idxs,
                                              failure_idxs);
  }
}

void LocallyRepairableCode::encode_partial_blocks_for_encoding_global(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs)
{
  std::vector<int> lrc_matrix((k + g + l) * k, 0);
  get_full_matrix(lrc_matrix.data(), k);
  make_encoding_matrix(&(lrc_matrix.data())[k * k]);
  encode_partial_blocks_for_encoding_(k, lrc_matrix.data(), data_ptrs, coding_ptrs,
                                      block_size, data_idxs, parity_idxs);

}

void LocallyRepairableCode::encode_partial_blocks_for_decoding_global(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs)
{
  std::vector<int> lrc_matrix((k + g + l) * k, 0);
  get_full_matrix(lrc_matrix.data(), k);
  make_encoding_matrix(&(lrc_matrix.data())[k * k]);
  encode_partial_blocks_for_decoding_(k, lrc_matrix.data(), data_ptrs, coding_ptrs,
                                      block_size, local_survivor_idxs,
                                      survivor_idxs, failure_idxs);
}

void LocallyRepairableCode::encode_partial_blocks_for_encoding_local(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs)
{
  int group_id = parity_idxs[0] - k - g;
  int min_idx = -1;
  int group_size = get_group_size(group_id, min_idx);

  std::vector<int> data_idxs_;
  std::vector<int> parity_idxs_;
  for (auto it = data_idxs.begin(); it != data_idxs.end(); it++) {
    int idx = *it;
    if (idx >= k + g)
      data_idxs_.push_back(group_size);
    else
      data_idxs_.push_back(idx - min_idx);
  }
  for (auto it = parity_idxs.begin(); it != parity_idxs.end(); it++){
    int idx = *it;
    if (idx >= k + g)
      parity_idxs_.push_back(group_size);
    else
      parity_idxs_.push_back(idx - min_idx);
  }

  std::vector<int> group_matrix((group_size + 1) * group_size, 0);
  get_full_matrix(group_matrix.data(), group_size);
  make_group_matrix(&(group_matrix.data())[group_size * group_size], group_id);
  encode_partial_blocks_for_encoding_(group_size, group_matrix.data(),
                                      data_ptrs, coding_ptrs, block_size,
                                      data_idxs_, parity_idxs_);
}

void LocallyRepairableCode::encode_partial_blocks_for_decoding_local(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs)
{
  int group_size = int(survivor_idxs.size());
  int group_id = -1;
  int min_idx = k + g + l;
  for (auto it = survivor_idxs.begin(); it != survivor_idxs.end(); it++) {
    int idx = *it;
    if (idx >= k + g)
      group_id = idx - k - g;
    if (idx < min_idx)
      min_idx = idx;
  }
  for (auto it = failure_idxs.begin(); it != failure_idxs.end(); it++) {
    int idx = *it;
    if (idx >= k + g)
      group_id = idx - k - g;
    if (idx < min_idx)
      min_idx = idx;
  }

  for (size_t i = 0; i < survivor_idxs.size(); ++i) {
    int idx = survivor_idxs[i];
    if (idx >= k + g)
        survivor_idxs[i] = group_size;
    else
        survivor_idxs[i] = idx - min_idx;
  }
  for (size_t i = 0; i < failure_idxs.size(); ++i) {
    int idx = failure_idxs[i];
    if (idx >= k + g)
        failure_idxs[i] = group_size;
    else
        failure_idxs[i] = idx - min_idx;
  }
  for (size_t i = 0; i < local_survivor_idxs.size(); ++i) {
    int idx = local_survivor_idxs[i];
    if (idx >= k + g)
        local_survivor_idxs[i] = group_size;
    else
        local_survivor_idxs[i] = idx - min_idx;
  }
  
  std::vector<int> group_matrix((group_size + 1) * group_size, 0);
  get_full_matrix(group_matrix.data(), group_size);
  make_group_matrix(&(group_matrix.data())[group_size * group_size], group_id);
  encode_partial_blocks_for_decoding_(group_size, group_matrix.data(),
                                      data_ptrs, coding_ptrs, block_size,
                                      local_survivor_idxs, survivor_idxs,
                                      failure_idxs);
}

void LocallyRepairableCode::partition_random()
{
  int n = k + l + g;
	std::vector<int> blocks;
  for(int i = 0; i < n; i++) {
    blocks.push_back(i);
  }

	int cnt = 0;
  while(cnt < n) {
		// at least subject to single-region fault tolerance
    int random_partition_size = random_range(1, g + 1);  
    int partition_size = std::min(random_partition_size, n - cnt);
    std::vector<int> partition;
    for(int i = 0; i < partition_size; i++, cnt++) {
      int ran_idx = random_index(n - cnt);
      int block_idx = blocks[ran_idx];
      partition.push_back(block_idx);
      auto it = std::find(blocks.begin(), blocks.end(), block_idx);
      blocks.erase(it);
    }
    partition_plan.push_back(partition);
  }
}

void LocallyRepairableCode::help_blocks_for_single_block_repair_oneoff(
                                int failure_idx, std::vector<std::vector<int>>& help_blocks)
{
	int parition_num = (int)partition_plan.size();
  if (!parition_num) {
    return;
  }

  if (local_or_column) {
    int gid = bid2gid(failure_idx);
    for (int i = 0; i < parition_num; i++) {
      std::vector<int> help_block;
      int cnt = 0;
      for (auto bid : partition_plan[i]) {
        if (bid2gid(bid) == gid && bid != failure_idx) {
          help_block.push_back(bid);
          cnt++;
        }
      }
      if (cnt > 0) {
        help_blocks.push_back(help_block);
      }
    }
  } else {
    int main_par_idx = 0;
	  std::vector<std::pair<int, int>> partition_idx_to_num;
    for (int i = 0; i < parition_num; i++) {
      int cnt = 0;
      for (auto bid : partition_plan[i]) {
        if (bid < k + g && bid != failure_idx) {
          cnt++;
        }
        if (bid == failure_idx) {
          main_par_idx = i;
          cnt = 0;
          break;
        }
      }
      if (cnt > 0) {
        partition_idx_to_num.push_back(std::make_pair(i, cnt));
      }
    }
    std::sort(partition_idx_to_num.begin(), partition_idx_to_num.end(),
						  cmp_descending);
    
    int cnt = 0;
    std::vector<int> main_help_block;
    for (auto idx : partition_plan[main_par_idx]) {
      if (idx != failure_idx && idx < k + g) {
        if (cnt < k) {
          main_help_block.push_back(idx);
          cnt++;
        } else {
          break;
        }
      }
    }
    if (cnt > 0) {
      help_blocks.push_back(main_help_block);
    }
    if (cnt == k) {
      return;
    }
    for (auto& pair : partition_idx_to_num) {
      std::vector<int> help_block;
      for (auto idx : partition_plan[pair.first]) {
        if (idx < k + g) {
          if (cnt < k) {
            help_block.push_back(idx);
            cnt++;
          } else {
            break;
          }
        }
      }
      if (cnt > 0 && cnt <= k) {
        help_blocks.push_back(help_block);
      }
      if (cnt == k) {
        return;
      }
    }
  }
}

void LocallyRepairableCode::help_blocks_for_multi_blocks_repair_oneoff(
						                    std::vector<int> failure_idxs,
						                    std::vector<std::vector<int>>& help_blocks)
{
  int failed_num = (int)failure_idxs.size();
  bool flag = false;
  for (auto failure_idx : failure_idxs) {
    if (failure_idx >= k + g) {
      flag = true;
    }
  }
  if (failed_num > g) {
    flag = true;
  }

  int parition_num = (int)partition_plan.size();
  if (parition_num == 0) {
    return;
  }

  std::vector<std::vector<int>> copy_partition;
	for (int i = 0; i < parition_num; i++) {
		std::vector<int> partition;
		for (auto idx : partition_plan[i]) {
			partition.push_back(idx);
		}
		copy_partition.push_back(partition);
	}

  if (flag) {
    for (auto failure_idx : failure_idxs) {
      for (int i = 0; i < parition_num; i++) {
        auto it = std::find(copy_partition[i].begin(), copy_partition[i].end(),
                            failure_idx);
        if (it != copy_partition[i].end()) {
          copy_partition[i].erase(it);	// remove the failures
          break;
        }			
      }
    }
    for (int i = 0; i < parition_num; i++) {
      if (copy_partition[i].size() > 0) {
        help_blocks.push_back(copy_partition[i]);
      }
    }
  } else {  // for repair with only data and global parity blocks
    int failures_cnt[parition_num] = {0};
    for (auto failure_idx : failure_idxs) {
      for (int i = 0; i < parition_num; i++) {
        auto it = std::find(copy_partition[i].begin(), copy_partition[i].end(),
                            failure_idx);
        if (it != copy_partition[i].end()) {
          failures_cnt[i]++;
          copy_partition[i].erase(it);	// remove the failures
          break;
        }			
      }
    }
    for (int l_ = k + g; l_ < k + g + l; l_++) {
      for (int i = 0; i < parition_num; i++) {
        auto it = std::find(copy_partition[i].begin(), copy_partition[i].end(), l_);
        if (it != copy_partition[i].end()) {
          copy_partition[i].erase(it);	// remove local parity blocks
          break;
        }	
      }
    }
    std::vector<std::pair<int, int>> main_partition_idx_to_num;
    std::vector<std::pair<int, int>> partition_idx_to_num;
    for (int i = 0; i < parition_num; i++) {
      int partition_size = (int)copy_partition[i].size();
      if (failures_cnt[i]) {
        main_partition_idx_to_num.push_back(std::make_pair(i, partition_size));
      } else {
        partition_idx_to_num.push_back(std::make_pair(i, partition_size));
      }
    }
    std::sort(main_partition_idx_to_num.begin(), main_partition_idx_to_num.end(),
              cmp_descending);
    std::sort(partition_idx_to_num.begin(), partition_idx_to_num.end(),
              cmp_descending);

    int cnt = 0;
    for (auto pair : main_partition_idx_to_num) {
      std::vector<int> main_help_block;
      for (auto idx : copy_partition[pair.first]) {
        if (cnt < k) {
          main_help_block.push_back(idx);
          cnt++;
        } else {
          break;
        }
      }
      if (cnt > 0 && cnt <= k && main_help_block.size() > 0) {
        help_blocks.push_back(main_help_block);
      }
      if (cnt == k) {
        return;
      }
    }
    for (auto& pair : partition_idx_to_num) {
      std::vector<int> help_block;
      for (auto idx : copy_partition[pair.first]) {
        if (cnt < k) {
          help_block.push_back(idx);
          cnt++;
        } else {
          break;
        }
      }
      if (cnt > 0 && cnt <= k && help_block.size() > 0) {
          help_blocks.push_back(help_block);
      } 
      if (cnt == k) {
        return;
      }
    }
  }
}

bool LocallyRepairableCode::generate_repair_plan(std::vector<int> failure_idxs,
																	               std::vector<RepairPlan>& plans)
{
  bool decodable = check_if_decodable(failure_idxs);
  if (!decodable) {
    return false;
  }
	int failed_num = (int)failure_idxs.size();
	if (failed_num == 1) {
    RepairPlan plan;
    for (auto idx : failure_idxs) {
      plan.failure_idxs.push_back(idx);
    }
    int group_id = bid2gid(failure_idxs[0]);
    if (group_id < l) {
      local_or_column = true;
      plan.local_or_column = true;
    } else {
      local_or_column = false;
      plan.local_or_column = false;
    }
		help_blocks_for_single_block_repair_oneoff(failure_idxs[0], plan.help_blocks);
    plans.push_back(plan);
	} else {
    std::vector<int> failed_map(k + g + l, 0);
    std::vector<int> fb_group_cnt(l + 1, 0);
    int num_of_failed_data_or_global = 0;
    int num_of_failed_blocks = int(failure_idxs.size());
    for (int i = 0; i < num_of_failed_blocks; i++) {
      int failed_idx = failure_idxs[i];
      failed_map[failed_idx] = 1;
      fb_group_cnt[bid2gid(failed_idx)] += 1;
      if (failed_idx < k + g) {
        num_of_failed_data_or_global += 1;
      }
    }

    int iter_cnt = 0;
    while (num_of_failed_blocks > 0) {
      for (int group_id = 0; group_id < l; group_id++) {
        // local repair
        if (fb_group_cnt[group_id] == 1) {
          int failed_idx = -1;
          for (int i = 0; i < k + g + l; i++) {
            if (failed_map[i] && bid2gid(i) == group_id) {
              failed_idx = i;
              break;
            }
          }
          RepairPlan plan;
          plan.local_or_column = true;
          plan.failure_idxs.push_back(failed_idx);
          local_or_column = true;
          help_blocks_for_single_block_repair_oneoff(failed_idx, plan.help_blocks);
          plans.push_back(plan);
          // update
          failed_map[failed_idx] = 0;
          fb_group_cnt[group_id] = 0;
          num_of_failed_blocks -= 1;
          if (failed_idx < k + g) {
            num_of_failed_data_or_global -= 1;
          }
        }
      }
      
      // 1 <= data_or_global_failed_num <= g, global repair
      if (num_of_failed_data_or_global > 0 && num_of_failed_data_or_global <= g) {
        RepairPlan plan;
        plan.local_or_column = false;

        for (int i = 0; i < k + g; i++) {
          if (failed_map[i]) {
            plan.failure_idxs.push_back(i);
          }
        }
        
        int tmp_failed_num = (int)plan.failure_idxs.size();
        if (tmp_failed_num == 1) {
          local_or_column = false;
          help_blocks_for_single_block_repair_oneoff(plan.failure_idxs[0], plan.help_blocks);
        } else {
          help_blocks_for_multi_blocks_repair_oneoff(plan.failure_idxs, plan.help_blocks);
        }
        plans.push_back(plan);

        // update 
        for (int i = 0; i < k + g; i++) {
          if (failed_map[i]) {
            failed_map[i] = 0;
            num_of_failed_blocks -= 1;
            fb_group_cnt[bid2gid(i)] -= 1;
          }
        }
        num_of_failed_data_or_global = 0;
      }

      if (iter_cnt > 0 && num_of_failed_blocks > 0) {
        bool if_decodable = true;
        if_decodable = check_if_decodable(failure_idxs);
        if (if_decodable) {  // if decodable, repair in one go
          RepairPlan plan;
          plan.local_or_column = false;
          for (int i = 0; i < k + g + l; i++) {
            if (failed_map[i]) {
              plan.failure_idxs.push_back(i);
            }
          }

          help_blocks_for_multi_blocks_repair_oneoff(plan.failure_idxs, plan.help_blocks);
          plans.push_back(plan);

          // update 
          for (int i = 0; i < k + g + l; i++) {
            if (failed_map[i]) {
              failed_map[i] = 0;
              num_of_failed_blocks -= 1;
              fb_group_cnt[bid2gid(i)] -= 1;
            }
          }
          num_of_failed_data_or_global = 0;
        } else {
          std::cout << "Undecodable!!!" << std::endl;
          return false;
        }
      }
      iter_cnt++;
    }
	}
  return true;
}

bool Azu_LRC::check_if_decodable(std::vector<int> failure_idxs)
{
  int failed_num = (int)failure_idxs.size();
  std::unordered_map<int, int> b2g;
  std::vector<int> group_fd_cnt;
  std::vector<int> group_slp_cnt;
  int sgp_cnt = g;
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k - i * r);
    for (int j = 0; j < group_size; j++) {
      b2g.insert(std::make_pair(idx, i));
      idx++;
    }
    b2g.insert(std::make_pair(k + g + i, i));
    group_fd_cnt.push_back(0);
    group_slp_cnt.push_back(1);
  }

  for (int i = 0; i < failed_num; i++) {
    int block_id = failure_idxs[i];
    if (block_id < k) {
      group_fd_cnt[b2g[block_id]] += 1;
    } else if (block_id < k + g && block_id >= k) {
      sgp_cnt -= 1;
    } else {
      group_slp_cnt[block_id - k - g] -= 1;
    }
  }
  for (int i = 0; i < l; i++) {
    if (group_slp_cnt[i] && group_slp_cnt[i] <= group_fd_cnt[i]) {
      group_fd_cnt[i] -= group_slp_cnt[i];
      group_slp_cnt[i] = 0;
    }
  }
  for (int i = 0; i < l; i++) {
    if (sgp_cnt >= group_fd_cnt[i]) {
      sgp_cnt -= group_fd_cnt[i];
      group_fd_cnt[i] = 0;
    } else {
      return false;
    }
  }
  return true;
}

void Azu_LRC::make_encoding_matrix(int *final_matrix)
{
  int *matrix = reed_sol_vandermonde_coding_matrix(k, g, w);

  bzero(final_matrix, sizeof(int) * k * (g + l));

  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++)
    {
      final_matrix[i * k + j] = matrix[i * k + j];
    }
  }

  for (int i = 0; i < l; i++) {
    for (int j = 0; j < k; j++) {
      if (i * r <= j && j < (i + 1) * r) {
        final_matrix[(i + g) * k + j] = 1;
      }
    }
  }

  free(matrix);
}

void Azu_LRC::make_group_matrix(int *group_matrix, int group_id)
{
  for (int i = 0; i < l; i++) {
    if (i == group_id) {
      int group_size = std::min(r, k - i * r);
      for(int j = 0; j < group_size; j++) {
        group_matrix[j] = 1;
      }
    }
  }
}

bool Azu_LRC::check_parameters()
{
  if (r * (l - 1) < k || !k || !l || !g)
    return false;
  return true;
}

int Azu_LRC::bid2gid(int block_id)
{
  int group_id = -1;
  if (block_id < k) {
    group_id = block_id / r;
  } else if (block_id >= k && block_id < k + g) {
    group_id = l; 
  } else {
    group_id = block_id - k - g;
  }
  return group_id;
}

int Azu_LRC::idxingroup(int block_id)
{
  if (block_id < k) {
    return block_id % r;
  } else if (block_id >= k && block_id < k + g) {
    return block_id - k;
  } else {
    if (block_id - k - g < l - 1) {
      return r;
    } else {
      return k % r == 0 ? r : k % r;
    }
  }
}

int Azu_LRC::get_group_size(int group_id, int& min_idx)
{
  min_idx = group_id * r;
  if (group_id < l - 1) {
    return r;
  } else if (group_id == l - 1) {
    return k % r == 0 ? r : k % r;
  } else {
    min_idx = k;
    return g;
  }
}

void Azu_LRC::grouping_information(std::vector<std::vector<int>>& groups)
{
  int idx = 0;
  for (int i = 0; i < l; i++) {
    std::vector<int> local_group;
    int group_size = std::min(r, k - i * r);
    for (int j = 0; j < group_size; j++) {
      local_group.push_back(idx++);
    }
    local_group.push_back(k + g + i);
    groups.push_back(local_group);
  }
  std::vector<int> local_group;
  for (int i = 0; i < g; i++) {
    local_group.push_back(idx++);
  }
  groups.push_back(local_group);
}

void Azu_LRC::partition_optimal()
{
  std::vector<std::vector<int>> stripe_groups;
  grouping_information(stripe_groups);
            
  std::vector<std::vector<int>> remaining_groups;
  for (int i = 0; i < l; i++)  {  // partition blocks in every local group
    std::vector<int> local_group = stripe_groups[i];
    int group_size = int(local_group.size());
    // every g + 1 blocks a partition
    for (int j = 0; j < group_size; j += g + 1) {
      if (j + g + 1 > group_size) { // derive the remain group
        std::vector<int> remain_group;
        for (int ii = j; ii < group_size; ii++) {
          remain_group.push_back(local_group[ii]);
        }
        remaining_groups.push_back(remain_group);
        break;
      }
      std::vector<int> partition;
      for (int ii = j; ii < j + g + 1; ii++) {
        partition.push_back(local_group[ii]);
      }
      partition_plan.push_back(partition);
    }
  }
  int theta = l;
  if ((r + 1) % (g + 1) > 1) {
    theta = g / ((r + 1) % (g + 1) - 1);
  }
  int remaining_groups_num = int(remaining_groups.size());
  for (int i = 0; i < remaining_groups_num; i += theta) { // organize every θ remaining groups
    std::vector<int> partition;
    for (int j = i; j < i + theta && j < remaining_groups_num; j++) {
      std::vector<int> remain_group = remaining_groups[j];
      int remain_block_num = int(remain_group.size());
      for (int ii = 0; ii < remain_block_num; ii++) {
        partition.push_back(remain_group[ii]);
      }
    }
    partition_plan.push_back(partition);
  }
  // calculate free space
  std::vector<std::pair<int, int>> space_left_in_each_partition;
  int sum_left_space = 0;
  for (int i = 0; i < (int)partition_plan.size(); i++) {
    int num_of_blocks = (int)partition_plan[i].size();
    int num_of_group = 0;
    for (int j = 0; j < num_of_blocks; j++) {
      if (partition_plan[i][j] >= k + g) {
        num_of_group += 1;
      }
    }
    if (num_of_group == 0) {
      num_of_group = 1;
    }
    int max_space = g + num_of_group;
    int left_space = max_space - num_of_blocks;
    space_left_in_each_partition.push_back({i, left_space});
    sum_left_space += left_space;
  }
  // place the global parity blocks
  int left_g = g;
  int global_idx = k;
  if (sum_left_space >= g) {  // insert to partitions with free space
    std::sort(space_left_in_each_partition.begin(), space_left_in_each_partition.end(), cmp_descending);
    for (auto i = 0; i < space_left_in_each_partition.size() && left_g > 0; i++) {
      if (space_left_in_each_partition[i].second > 0) {
        int j = space_left_in_each_partition[i].first;
        int left_space = space_left_in_each_partition[i].second;
        if (left_g >= left_space) {
          left_g -= left_space;
        } else {
          left_space = left_g;
          left_g -= left_g;
        }
        while (left_space--) {
          partition_plan[j].push_back(global_idx++);
        }
      }
    }
    my_assert(left_g == 0);
  } else {  // a seperate new partition
    std::vector<int> partition;
    while (global_idx < k + g) {
      partition.push_back(global_idx++);
    }
    partition_plan.push_back(partition);
  }
}

void Azu_LRC::partition_sub_optimal()
{
  std::vector<std::vector<int>> stripe_groups;
  grouping_information(stripe_groups);

  std::vector<std::vector<int>> remaining_groups;
  for (int i = 0; i < l; i++) { // partition blocks in every local group
    std::vector<int> local_group = stripe_groups[i];
    int group_size = int(local_group.size());
    // every g + 1 blocks a partition
    for (int j = 0; j < group_size; j += g + 1) {
      if (j + g + 1 > group_size) { // derive the remain group
        std::vector<int> remain_group;
        for (int ii = j; ii < group_size; ii++) {
          remain_group.push_back(local_group[ii]);
        }
        remaining_groups.push_back(remain_group);
        break;
      }
      std::vector<int> partition;
      for (int ii = j; ii < j + g + 1; ii++) {
        partition.push_back(local_group[ii]);
      }
      partition_plan.push_back(partition);
    }
  }

  int theta = l;
  if ((r + 1) % (g + 1) > 1) {
    theta = g / ((r + 1) % (g + 1) - 1);
  }
  int remaining_groups_num = int(remaining_groups.size());
  for (int i = 0; i < remaining_groups_num; i += theta) { // organize every θ remaining groups
    std::vector<int> partition;
    for (int j = i; j < i + theta && j < remaining_groups_num; j++) {
      std::vector<int> remain_group = remaining_groups[j];
      int remain_block_num = int(remain_group.size());
      for (int ii = 0; ii < remain_block_num; ii++) {
        partition.push_back(remain_group[ii]);
      }
    }
    partition_plan.push_back(partition);
  }

  // organize the global parity blocks in a single partition
  if (theta == remaining_groups_num) {
    int idx = (int)partition_plan.size() - 1;
    for (int i = k; i < k + g; i++) {
      partition_plan[idx].push_back(i);
    }
  } else {
    std::vector<int> partition;
    for (int i = k; i < k + g; i++) {
      partition.push_back(i);
    }
    partition_plan.push_back(partition);
  }
}

std::string Azu_LRC::self_information()
{
	return "Azure_LRC(" + std::to_string(k) + "," + std::to_string(l) + \
         "," + std::to_string(g) + ")";
}

bool Azu_LRC_1::check_if_decodable(std::vector<int> failure_idxs)
{
  int failed_num = (int)failure_idxs.size();
  std::unordered_map<int, int> b2g;
  std::vector<int> group_fd_cnt;
  std::vector<int> group_slp_cnt;
  int sgp_cnt = g;
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k - i * r);
    for (int j = 0; j < group_size; j++) {
      b2g.insert(std::make_pair(idx, i));
      idx++;
    }
    b2g.insert(std::make_pair(k + g + i, i));
    group_fd_cnt.push_back(0);
    group_slp_cnt.push_back(1);
  }

  for (int i = 0; i < failed_num; i++) {
    int block_id = failure_idxs[i];
    if (block_id < k) {
      group_fd_cnt[b2g[block_id]] += 1;
    } else if (block_id < k + g && block_id >= k) {
      sgp_cnt -= 1;
    } else {
      group_slp_cnt[block_id - k - g] -= 1;
    }
  }
  for (int i = 0; i < l; i++) {
    if (i < l - 1) {
      if (group_slp_cnt[i] && group_slp_cnt[i] <= group_fd_cnt[i]) {
        group_fd_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
      }
    } else {
      if (group_slp_cnt[i] && sgp_cnt == g - 1) {
        sgp_cnt += 1;
      }
    }
  }
  for (int i = 0; i < l; i++) {
    if (sgp_cnt >= group_fd_cnt[i]) {
      sgp_cnt -= group_fd_cnt[i];
      group_fd_cnt[i] = 0;
    } else {
      return false;
    }
  }
  return true;
}

void Azu_LRC_1::make_encoding_matrix(int *final_matrix)
{
  int *matrix = reed_sol_vandermonde_coding_matrix(k, g, w);

  bzero(final_matrix, sizeof(int) * k * (g + l));

  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[i * k + j] = matrix[i * k + j];
    }
  }

  std::vector<int> l_matrix(l * (k + g), 0);
  std::vector<int> d_g_matrix((k + g) * k, 0);
  int idx = 0;
  for (int i = 0; i < l - 1; i++) {
    int group_size = std::min(r, k - i * r);
    for (int j = 0; j < group_size; j++) {
      l_matrix[i * (k + g) + idx] = 1;
      idx++;
    }
  }
  for (int j = 0; j < g; j++) {
    l_matrix[(l - 1) * (k + g) + idx] = 1;
    idx++;
  }
  for (int i = 0; i < k; i++) {
    d_g_matrix[i * k + i] = 1;
  }
  idx = k * k;
  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      d_g_matrix[idx + i * k + j] = matrix[i * k + j];
    }
  }

  int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(),
                                             l, k + g, k + g, k, w);

  idx = g * k;
  for (int i = 0; i < l; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
    }
  }

  free(matrix);
  free(mix_matrix);
}

void Azu_LRC_1::make_group_matrix(int *group_matrix, int group_id)
{
  if (group_id == l - 1) {
    for (int j = 0; j < g; j++) {
      group_matrix[j] = 1;
    }
    return;
  }
  for (int i = 0; i < l - 1; i++) {
    if (i == group_id) {
      int group_size = std::min(r, k - i * r);
      for (int j = 0; j < group_size; j++) {
        group_matrix[j] = 1;
      }
    }
  }
}

bool Azu_LRC_1::check_parameters()
{
  if (r * (l - 2) < k || !k || !l || !g)
    return false;
  return true;
}

int Azu_LRC_1::bid2gid(int block_id)
{
  int group_id = -1;
  if (block_id < k) {
    group_id = block_id / r;
  } else if (block_id >= k && block_id < k + g) {
    group_id = l - 1; 
  } else {
    group_id = block_id - k - g;
  }
  return group_id;
}

int Azu_LRC_1::idxingroup(int block_id)
{
  if (block_id < k) {
    return block_id % r;
  } else if (block_id >= k && block_id < k + g) {
    return block_id - k;
  } else {
    if (block_id - k - g < l - 2) {
      return r;
    } else if(block_id - k - g == l - 2) {
      return k % r == 0 ? r : k % r;
    } else {
      return g;
    }
  }
}

int Azu_LRC_1::get_group_size(int group_id, int& min_idx)
{
  min_idx = group_id * r;
  if (group_id < l - 2) {
    return r;
  } else if (group_id == l - 2) {
    return k % r == 0 ? r : k % r;;
  } else {
    min_idx = k;
    return g;
  }
}

void Azu_LRC_1::grouping_information(std::vector<std::vector<int>>& groups)
{
  int idx = 0;
  for (int i = 0; i < l - 1; i++) {
    std::vector<int> local_group;
    int group_size = std::min(r, k - i * r);
    for (int j = 0; j < group_size; j++) {
      local_group.push_back(idx++);
    }
    local_group.push_back(k + g + i);
    groups.push_back(local_group);
  }
  std::vector<int> local_group;
  for (int i = 0; i < g; i++) {
    local_group.push_back(idx++);
  }
  local_group.push_back(k + g + l - 1);
  groups.push_back(local_group);
}

void Azu_LRC_1::partition_optimal()
{
  std::vector<std::vector<int>> stripe_groups;
  grouping_information(stripe_groups);

  for (int i = 0; i < l; i++) {
    std::vector<int> local_group = stripe_groups[i];
    int group_size = int(local_group.size());
    // every g + 1 blocks a partition
    for (int j = 0; j < group_size; j += g + 1) {
      std::vector<int> partition;
      for (int ii = j; ii < j + g + 1 && ii < group_size; ii++) {
        partition.push_back(local_group[ii]);
      }
      partition_plan.push_back(partition);
    }
  }
}

std::string Azu_LRC_1::self_information()
{
	return "Azure_LRC+1(" + std::to_string(k) + "," + std::to_string(l) + \
         "," + std::to_string(g) + ")";
}

bool Opt_LRC::check_if_decodable(std::vector<int> failure_idxs)
{
  int failed_num = (int)failure_idxs.size();
  std::unordered_map<int, int> b2g;
  std::vector<int> group_fd_cnt;
  std::vector<int> group_fgp_cnt;
  std::vector<int> group_slp_cnt;
  std::vector<bool> group_pure_flag;
  int sgp_cnt = g;
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k + g - i * r);
    for (int j = 0; j < group_size; j++) {
      b2g.insert(std::make_pair(idx, i));
      idx++;
    }
    if (idx <= k || idx - group_size >= k) {
      group_pure_flag.push_back(true);
    } else {
      group_pure_flag.push_back(false);
    }
    b2g.insert(std::make_pair(k + g + i, i));
    group_fd_cnt.push_back(0);
    group_fgp_cnt.push_back(0);
    group_slp_cnt.push_back(1);
  }

  for (int i = 0; i < failed_num; i++) {
    int block_id = failure_idxs[i];
    if (block_id < k) {
      group_fd_cnt[b2g[block_id]] += 1;
    } else if (block_id < k + g && block_id >= k) {
      group_fgp_cnt[b2g[block_id]] += 1;
      sgp_cnt -= 1;
    } else {
      group_slp_cnt[block_id - k - g] -= 1;
    }
  }

  for (int i = 0; i < l; i++) {
    if (group_slp_cnt[i] && group_pure_flag[i]) {
      if (group_slp_cnt[i] <= group_fd_cnt[i]) {
        group_fd_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
      }
      if (group_slp_cnt[i] && group_slp_cnt[i] == group_fgp_cnt[i]) {
        group_fgp_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
        sgp_cnt += 1;
      }
    } else if (group_slp_cnt[i] && !group_pure_flag[i]) {
      if (group_fd_cnt[i] == 1 && !group_fgp_cnt[i]) {
        group_fd_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
      } else if (group_fgp_cnt[i] == 1 && !group_fd_cnt[i]) {
        group_fgp_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
        sgp_cnt += 1;
      }
    }
  }
  for (int i = 0; i < l; i++) {
    if (sgp_cnt >= group_fd_cnt[i]) {
      sgp_cnt -= group_fd_cnt[i];
      group_fd_cnt[i] = 0;
    } else {
      return false;
    }
  }
  return true;
}

void Opt_LRC::make_encoding_matrix(int *final_matrix)
{
  int *matrix = reed_sol_vandermonde_coding_matrix(k, g, w);

  bzero(final_matrix, sizeof(int) * k * (g + l));

  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[i * k + j] = matrix[i * k + j];
    }
  }

  std::vector<int> l_matrix(l * (k + g), 0);
  std::vector<int> d_g_matrix((k + g) * k, 0);
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k + g - i * r);
    for (int j = 0; j < group_size; j++) {
      l_matrix[i * (k + g) + idx] = 1;
      idx++;
    }
  }
  for (int i = 0; i < k; i++) {
    d_g_matrix[i * k + i] = 1;
  }
  idx = k * k;
  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      d_g_matrix[idx + i * k + j] = matrix[i * k + j];
    }
  }

  // print_matrix(l_matrix.data(), l, k + g, "l_matrix");
  // print_matrix(d_g_matrix.data(), k + g, k, "d_g_matrix");

  int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(),
                                             l, k + g, k + g, k, w);

  idx = g * k;
  for (int i = 0; i < l; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
    }
  }

  free(matrix);
  free(mix_matrix);
}

void Opt_LRC::make_group_matrix(int *group_matrix, int group_id)
{
  for (int i = 0; i < l; i++) {
    if (i == group_id) {
      int group_size = std::min(r, k + g - i * r);
      for (int j = 0; j < group_size; j++) {
        group_matrix[j] = 1;
      }
    }
  }
}

bool Opt_LRC::check_parameters()
{
  if (r * (l - 1) < k + g || !k || !l || !g)
    return false;
  return true;
}

int Opt_LRC::bid2gid(int block_id)
{
  int group_id = -1;
  if (block_id < k + g) {
    group_id = block_id / r;
  } else {
    group_id = block_id - k - g;
  }
  return group_id;
}

int Opt_LRC::idxingroup(int block_id)
{
  if (block_id < k + g) {
    return block_id % r;
  } else {
    if (block_id - k - g < l - 1) {
      return r;
    } else {
      return (k + g) % r == 0 ? r : (k + g) % r;
    }
  }
}

int Opt_LRC::get_group_size(int group_id, int& min_idx)
{
  min_idx = group_id * r;
  if (group_id < l - 1) {
    return r;
  } else {
    return (k + g) % r == 0 ? r : (k + g) % r;
  }
}

void Opt_LRC::grouping_information(std::vector<std::vector<int>>& groups)
{
  int idx = 0;
  for (int i = 0; i < l; i++) {
    std::vector<int> local_group;
    int group_size = std::min(r, k + g - i * r);
    for (int j = 0; j < group_size; j++) {
      local_group.push_back(idx++);
    }
    local_group.push_back(k + g + i);
    groups.push_back(local_group);
  }
}

void Opt_LRC::partition_optimal()
{
  std::vector<std::vector<int>> stripe_groups;
  grouping_information(stripe_groups);

  for (int i = 0; i < l; i++) {
    std::vector<int> local_group = stripe_groups[i];
    int group_size = int(local_group.size());
    // every g + 1 blocks a partition
    for (int j = 0; j < group_size; j += g + 1) {
      std::vector<int> partition;
      for (int ii = j; ii < j + g + 1 && ii < group_size; ii++) {
        partition.push_back(local_group[ii]);
      }
      partition_plan.push_back(partition);
    }
  }
}

std::string Opt_LRC::self_information()
{
	return "Optimal_LRC(" + std::to_string(k) + "," + std::to_string(l) + \
         "," + std::to_string(g) + ")";
}

void Opt_Cau_LRC::encode_partial_blocks_for_encoding_local(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs)
{
  int group_id = parity_idxs[0] - k - g;
  int min_idx = -1;
  int group_size = get_group_size(group_id, min_idx);

  std::vector<int> data_idxs_;
  std::vector<int> parity_idxs_;
  for (auto it = data_idxs.begin(); it != data_idxs.end(); it++) {
    int idx = *it;
    if (idx >= k + g) {
      data_idxs_.push_back(group_size);
    } else if (idx >= k && idx < k + g) {
      data_idxs_.push_back(group_size - g + idx - k);
    } else {
      data_idxs_.push_back(idx - min_idx);
    }
  }
  for (auto it = parity_idxs.begin(); it != parity_idxs.end(); it++){
    int idx = *it;
    if (idx >= k + g) {
      parity_idxs_.push_back(group_size);
    } else if (idx >= k && idx < k + g) {
      parity_idxs_.push_back(group_size - g + idx - k);
    } else {
      parity_idxs_.push_back(idx - min_idx);
    }
  }

  std::vector<int> group_matrix((group_size + 1) * group_size, 0);
  get_full_matrix(group_matrix.data(), group_size);
  make_group_matrix(&(group_matrix.data())[group_size * group_size], group_id);
  encode_partial_blocks_for_encoding_(group_size, group_matrix.data(),
                                      data_ptrs, coding_ptrs, block_size,
                                      data_idxs_, parity_idxs_);
}

void Opt_Cau_LRC::encode_partial_blocks_for_decoding_local(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs)
{
  int group_size = int(survivor_idxs.size());
  int group_id = -1;
  int min_idx = k + g + l;
  for (auto it = survivor_idxs.begin(); it != survivor_idxs.end(); it++) {
    int idx = *it;
    if (idx >= k + g)
      group_id = idx - k - g;
    if (idx < min_idx)
      min_idx = idx;
  }
  for (auto it = failure_idxs.begin(); it != failure_idxs.end(); it++) {
    int idx = *it;
    if (idx >= k + g)
      group_id = idx - k - g;
    if (idx < min_idx)
      min_idx = idx;
  }

  // change into the index for encoding a local parity block
  std::vector<int> survivor_idxs_;
  std::vector<int> local_survivor_idxs_;
  std::vector<int> failure_idxs_;
  for (auto it = survivor_idxs.begin(); it != survivor_idxs.end(); it++) {
    int idx = *it;
    if (idx >= k + g) {
      survivor_idxs_.push_back(group_size);
    } else if (idx >= k && idx < k + g) {
      survivor_idxs_.push_back(group_size - g + idx - k);
    } else {
      survivor_idxs_.push_back(idx - min_idx);
    }
  }
  for (auto it = failure_idxs.begin(); it != failure_idxs.end(); it++){
    int idx = *it;
    if (idx >= k + g) {
      failure_idxs_.push_back(group_size);
    } else if (idx >= k && idx < k + g) {
      failure_idxs_.push_back(group_size - g + idx - k);
    } else {
      failure_idxs_.push_back(idx - min_idx);
    }
  }
  for (auto it = local_survivor_idxs.begin(); it != local_survivor_idxs.end(); it++) {
    int idx = *it;
    if (idx >= k + g) {
      local_survivor_idxs_.push_back(group_size);
    } else if (idx >= k && idx < k + g) {
      local_survivor_idxs_.push_back(group_size - g + idx - k);
    } else {
      local_survivor_idxs_.push_back(idx - min_idx);
    }
  }

  std::vector<int> group_matrix((group_size + 1) * group_size, 0);
  get_full_matrix(group_matrix.data(), group_size);
  make_group_matrix(&(group_matrix.data())[group_size * group_size], group_id);
  encode_partial_blocks_for_decoding_(group_size, group_matrix.data(),
                                      data_ptrs, coding_ptrs, block_size,
                                      local_survivor_idxs_, survivor_idxs_,
                                      failure_idxs_);
}

bool Opt_Cau_LRC::check_if_decodable(std::vector<int> failure_idxs)
{
  int failed_num = (int)failure_idxs.size();
  std::unordered_map<int, int> b2g;
  std::vector<int> group_fd_cnt;
  std::vector<int> group_slp_cnt;
  int fd_cnt = 0;
  int sgp_cnt = g;
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k - i * r);
    for (int j = 0; j < group_size; j++) {
      b2g.insert(std::make_pair(idx, i));
      idx++;
    }
    b2g.insert(std::make_pair(k + g + i, i));
    group_fd_cnt.push_back(0);
    group_slp_cnt.push_back(1);
  }

  for (int i = 0; i < failed_num; i++) {
    int block_id = failure_idxs[i];
    if (block_id < k) {
      group_fd_cnt[b2g[block_id]] += 1;
      fd_cnt += 1;
    } else if (block_id < k + g && block_id >= k) {
      sgp_cnt -= 1;
    } else {
      group_slp_cnt[block_id - k - g] -= 1;
    }
  }

  if (sgp_cnt < g) {
    int fg_cnt = g - sgp_cnt;
    int healthy_group_cnt = 0;
    for (int i = 0; i < l; i++) {
      if (group_slp_cnt[i] && !group_fd_cnt[i]) {
        healthy_group_cnt++;
      }
    }
    if (healthy_group_cnt >= fg_cnt) {  // repair failed global parity blocks by enough healthy groups
      sgp_cnt = g;
    }
  }

  if (sgp_cnt < g) {
    if (sgp_cnt >= fd_cnt) {
      return true;
    } else {
      return false;
    }
  } else {
    for (int i = 0; i < l; i++) {
      if (group_slp_cnt[i] && group_slp_cnt[i] <= group_fd_cnt[i]) {
        group_fd_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
      }
    }
    for (int i = 0; i < l; i++) {
      if (sgp_cnt >= group_fd_cnt[i]) {
        sgp_cnt -= group_fd_cnt[i];
        group_fd_cnt[i] = 0;
      } else {
        return false;
      }
    }
  }
  return true;
}

void Opt_Cau_LRC::make_encoding_matrix(int *final_matrix)
{
  int *matrix = cauchy_good_general_coding_matrix(k, g + 1, w);

  bzero(final_matrix, sizeof(int) * k * (g + l));

  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[i * k + j] = matrix[i * k + j];
    }
  }

  int d_idx = 0;
  for (int i = 0; i < l; i++) {
    for (int j = 0; j < k; j++) {
      if (i * r <= j && j < (i + 1) * r) {
        final_matrix[(i + g) * k + j] = matrix[g * k + d_idx];
        d_idx++;
      }
    }
  }

  // print_matrix(final_matrix, g + l, k, "final_matrix_before_xor");

  for (int i = 0; i < l; i++) {
    for (int j = 0; j < g; j++) {
      galois_region_xor((char *)&matrix[j * k], (char *)&final_matrix[(i + g) * k], 4 * k);
    }
  }

  // print_matrix(final_matrix, g + l, k, "final_matrix_after_xor");

  free(matrix);
}

void Opt_Cau_LRC::make_encoding_matrix_v2(int *final_matrix)
{
  int *matrix = cauchy_good_general_coding_matrix(k, g + 1, w);

  bzero(final_matrix, sizeof(int) * k * (g + l));

  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[i * k + j] = matrix[i * k + j];
    }
  }

  std::vector<int> l_matrix(l * (k + g), 0);
  std::vector<int> d_g_matrix((k + g) * k, 0);
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k - i * r);
    for (int j = 0; j < group_size; j++) {
      l_matrix[i * (k + g) + idx] = matrix[g * k + idx];
      idx++;
    }
    for (int j = k; j < k + g; j++) {
      l_matrix[i * (k + g) + j] = 1;
    }
  }
  for (int i = 0; i < k; i++) {
    d_g_matrix[i * k + i] = 1;
  }
  idx = k * k;
  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      d_g_matrix[idx + i * k + j] = matrix[i * k + j];
    }
  }

  // print_matrix(l_matrix.data(), l, k + g, "l_matrix");
  // print_matrix(d_g_matrix.data(), k + g, k, "d_g_matrix");

  int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(),
                                             l, k + g, k + g, k, w);

  idx = g * k;
  for (int i = 0; i < l; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
    }
  }

  // print_matrix(final_matrix, g + l, k, "final_matrix");

  free(matrix);
  free(mix_matrix);
}

void Opt_Cau_LRC::make_group_matrix(int *group_matrix, int group_id)
{
  int *matrix = cauchy_good_general_coding_matrix(k, g + 1, 8);
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k - i * r);
    for (int j = 0; j < group_size; j++) {
      if (i == group_id) {
        group_matrix[j] = matrix[g * k + idx];
      }
      idx++;
    }
    for (int j = group_size; j < group_size + g; j++) {
      if (i == group_id)
        group_matrix[j] = 1;
    }
  }
}

bool Opt_Cau_LRC::check_parameters()
{
  if (r * (l - 1) < k || !k || !l || !g)
    return false;
  return true;
}

int Opt_Cau_LRC::bid2gid(int block_id)
{
  int group_id = -1;
  if (block_id < k) {
    group_id = block_id / r;
  } else if (block_id >= k && block_id < k + g) {
    group_id = l; 
  } else {
    group_id = block_id - k - g;
  }
  return group_id;
}

int Opt_Cau_LRC::idxingroup(int block_id)
{
  if (block_id < k) {
    return block_id % r;
  } else if (block_id >= k && block_id < k + g) {
    return block_id - k + r;
  } else {
    if (block_id - k - g < l - 1) {
      return r + g;
    } else {
      return k % r == 0 ? r + g : k % r + g;
    }
  }
}

int Opt_Cau_LRC::get_group_size(int group_id, int& min_idx)
{
  min_idx = group_id * r;
  if (group_id < l - 1) {
    return r + g;
  } else if (group_id == l - 1) {
    return (k % r == 0 ? r : k % r) + g;
  } else {
    min_idx = k;
    return g;
  }
}

void Opt_Cau_LRC::grouping_information(std::vector<std::vector<int>>& groups)
{
  int idx = 0;
  for (int i = 0; i < l; i++) {
    std::vector<int> local_group;
    int group_size = std::min(r, k - i * r);
    for (int j = 0; j < group_size; j++) {
      local_group.push_back(idx++);
    }
    local_group.push_back(k + g + i);
    groups.push_back(local_group);
  }
  std::vector<int> local_group;
  for (int i = 0; i < g; i++) {
    local_group.push_back(idx++);
  }
  groups.push_back(local_group);
}

void Opt_Cau_LRC::partition_optimal()
{
  std::vector<std::vector<int>> stripe_groups;
  grouping_information(stripe_groups);
            
  std::vector<std::vector<int>> remaining_groups;
  for (int i = 0; i < l; i++)  {  // partition blocks in every local group
    std::vector<int> local_group = stripe_groups[i];
    int group_size = int(local_group.size());
    // every g + 1 blocks a partition
    for (int j = 0; j < group_size; j += g + 1) {
      if (j + g + 1 > group_size) { // derive the remain group
        std::vector<int> remain_group;
        for (int ii = j; ii < group_size; ii++) {
          remain_group.push_back(local_group[ii]);
        }
        remaining_groups.push_back(remain_group);
        break;
      }
      std::vector<int> partition;
      for (int ii = j; ii < j + g + 1; ii++) {
        partition.push_back(local_group[ii]);
      }
      partition_plan.push_back(partition);
    }
  }
  int theta = l;
  if ((r + 1) % (g + 1) > 1) {
    theta = g / ((r + 1) % (g + 1) - 1);
  }
  int remaining_groups_num = int(remaining_groups.size());
  for (int i = 0; i < remaining_groups_num; i += theta) { // organize every θ remaining groups
    std::vector<int> partition;
    for (int j = i; j < i + theta && j < remaining_groups_num; j++) {
      std::vector<int> remain_group = remaining_groups[j];
      int remain_block_num = int(remain_group.size());
      for (int ii = 0; ii < remain_block_num; ii++) {
        partition.push_back(remain_group[ii]);
      }
    }
    partition_plan.push_back(partition);
  }
  // calculate free space
  std::vector<std::pair<int, int>> space_left_in_each_partition;
  int sum_left_space = 0;
  for (int i = 0; i < (int)partition_plan.size(); i++) {
    int num_of_blocks = (int)partition_plan[i].size();
    int num_of_group = 0;
    for (int j = 0; j < num_of_blocks; j++) {
      if (partition_plan[i][j] >= k + g) {
        num_of_group += 1;
      }
    }
    if (num_of_group == 0) {
      num_of_group = 1;
    }
    int max_space = g + num_of_group;
    int left_space = max_space - num_of_blocks;
    space_left_in_each_partition.push_back({i, left_space});
    sum_left_space += left_space;
  }
  // place the global parity blocks
  int left_g = g;
  int global_idx = k;
  if (sum_left_space >= g) {  // insert to partitions with free space
    std::sort(space_left_in_each_partition.begin(), space_left_in_each_partition.end(), cmp_descending);
    for (auto i = 0; i < space_left_in_each_partition.size() && left_g > 0; i++) {
      if (space_left_in_each_partition[i].second > 0) {
        int j = space_left_in_each_partition[i].first;
        int left_space = space_left_in_each_partition[i].second;
        if (left_g >= left_space) {
          left_g -= left_space;
        } else {
          left_space = left_g;
          left_g -= left_g;
        }
        while (left_space--) {
          partition_plan[j].push_back(global_idx++);
        }
      }
    }
    my_assert(left_g == 0);
  } else {  // a seperate new partition
    std::vector<int> partition;
    while (global_idx < k + g) {
      partition.push_back(global_idx++);
    }
    partition_plan.push_back(partition);
  }
}

std::string Opt_Cau_LRC::self_information()
{
	return "Optimal_Cauchy_LRC(" + std::to_string(k) + "," + std::to_string(l) + \
         "," + std::to_string(g) + ")";
}

void Opt_Cau_LRC::help_blocks_for_single_block_repair_oneoff(
                      int failure_idx, std::vector<std::vector<int>>& help_blocks)
{
	int parition_num = (int)partition_plan.size();
  if (!parition_num) {
    return;
  }

  if (local_or_column) {
    if (failure_idx >= k && failure_idx < k + g) {
      for (int i = 0; i < parition_num; i++) {
        std::vector<int> help_block;
        int cnt = 0;
        for (auto bid : partition_plan[i]) {
          if ((bid >= k && bid < k + g && bid != failure_idx) || 
              bid2gid(bid) == surviving_group_id) {
            help_block.push_back(bid);
            cnt++;
          }
        }
        if (cnt > 0) {
          help_blocks.push_back(help_block);
        }
      }
    } else {
      int gid = bid2gid(failure_idx);
      for (int i = 0; i < parition_num; i++) {
        std::vector<int> help_block;
        int cnt = 0;
        for (auto bid : partition_plan[i]) {
          if ((bid2gid(bid) == gid && bid != failure_idx) || 
              (bid >= k && bid < k + g)) {
            help_block.push_back(bid);
            cnt++;
          }
        }
        if (cnt > 0) {
          help_blocks.push_back(help_block);
        }
      }
    }
  } else {
    int main_par_idx = 0;
	  std::vector<std::pair<int, int>> partition_idx_to_num;
    for (int i = 0; i < parition_num; i++) {
      int cnt = 0;
      for (auto bid : partition_plan[i]) {
        if (bid < k + g && bid != failure_idx) {
          cnt++;
        }
        if (bid == failure_idx) {
          main_par_idx = i;
          cnt = 0;
          break;
        }
      }
      if (cnt > 0) {
        partition_idx_to_num.push_back(std::make_pair(i, cnt));
      }
    }
    std::sort(partition_idx_to_num.begin(), partition_idx_to_num.end(),
						  cmp_descending);
    
    int cnt = 0;
    std::vector<int> main_help_block;
    for (auto idx : partition_plan[main_par_idx]) {
      if (idx != failure_idx && idx < k + g) {
        if (cnt < k) {
          main_help_block.push_back(idx);
          cnt++;
        } else {
          break;
        }
      }
    }
    if (cnt > 0) {
      help_blocks.push_back(main_help_block);
    }
    if (cnt == k) {
      return;
    }
    for (auto& pair : partition_idx_to_num) {
      std::vector<int> help_block;
      for (auto idx : partition_plan[pair.first]) {
        if (idx < k + g) {
          if (cnt < k) {
            help_block.push_back(idx);
            cnt++;
          } else {
            break;
          }
        }
        
      }
      if (cnt > 0 && cnt <= k) {
        help_blocks.push_back(help_block);
      }
      if (cnt == k) {
        return;
      }
    }
  }
}

bool Opt_Cau_LRC::generate_repair_plan(std::vector<int> failure_idxs,
																	     std::vector<RepairPlan>& plans)
{
  bool decodable = check_if_decodable(failure_idxs);
  if (!decodable) {
    return false;
  }
	int failed_num = (int)failure_idxs.size();
	if (failed_num == 1) {
    RepairPlan plan;
    for (auto idx : failure_idxs) {
      plan.failure_idxs.push_back(idx);
    }
    local_or_column = true;
    plan.local_or_column = true;
		help_blocks_for_single_block_repair_oneoff(failure_idxs[0], plan.help_blocks);
    plans.push_back(plan);
	} else {
    std::vector<int> failed_map(k + g + l, 0);
    std::vector<int> fb_group_cnt(l + 1, 0);
    int num_of_failed_data_or_global = 0;
    int num_of_failed_blocks = int(failure_idxs.size());
    for (int i = 0; i < num_of_failed_blocks; i++) {
      int failed_idx = failure_idxs[i];
      failed_map[failed_idx] = 1;
      fb_group_cnt[bid2gid(failed_idx)] += 1;
      if (failed_idx < k + g) {
        num_of_failed_data_or_global += 1;
        if (failed_idx >= k) {
          for (int j = 0; j < l; j++) {
            fb_group_cnt[j] += 1;
          }
        }
      }
    }

    int iter_cnt = 0;
    while (num_of_failed_blocks > 0) {
      for (int i = 0; i < k + g + l; i++) {
        if (i >= k && i < k + g && failed_map[i]) {
          for (int j = 0; j < l; j++) {
            if (fb_group_cnt[j] == 1) {
              RepairPlan plan;
              plan.local_or_column = true;
              plan.failure_idxs.push_back(i);
              local_or_column = true;
              surviving_group_id = j;
              help_blocks_for_single_block_repair_oneoff(i, plan.help_blocks);
              plans.push_back(plan);

              // update
              failed_map[i] = 0;
              for (int jj = 0; jj <= l; jj++) {
                fb_group_cnt[jj] -= 1;
              }
              num_of_failed_blocks -= 1;
              num_of_failed_data_or_global -= 1;
              break;
            }
          }
        }
      }
      for (int group_id = 0; group_id < l; group_id++) {
        // local repair
        if (fb_group_cnt[group_id] == 1) {
          int failed_idx = -1;
          for (int i = 0; i < k + g + l; i++) {
            if (failed_map[i] && bid2gid(i) == group_id) {
              failed_idx = i;
              break;
            }
          }
          RepairPlan plan;
          plan.local_or_column = true;
          plan.failure_idxs.push_back(failed_idx);
          local_or_column = true;
          help_blocks_for_single_block_repair_oneoff(failed_idx, plan.help_blocks);
          plans.push_back(plan);
          // update
          failed_map[failed_idx] = 0;
          fb_group_cnt[group_id] = 0;
          num_of_failed_blocks -= 1;
          if (failed_idx < k + g) {
            num_of_failed_data_or_global -= 1;
          }
        }
      }
      
      // 1 <= data_or_global_failed_num <= g, global repair
      if (num_of_failed_data_or_global > 0 && num_of_failed_data_or_global <= g) {
        RepairPlan plan;
        plan.local_or_column = false;

        for (int i = 0; i < k + g; i++) {
          if (failed_map[i]) {
            plan.failure_idxs.push_back(i);
          }
        }
        
        int tmp_failed_num = (int)plan.failure_idxs.size();
        if (tmp_failed_num == 1) {
          local_or_column = false;
          help_blocks_for_single_block_repair_oneoff(plan.failure_idxs[0], plan.help_blocks);
        } else {
          help_blocks_for_multi_blocks_repair_oneoff(plan.failure_idxs, plan.help_blocks);
        }
        plans.push_back(plan);

        // update 
        for (int i = 0; i < k + g; i++) {
          if (failed_map[i]) {
            failed_map[i] = 0;
            num_of_failed_blocks -= 1;
            fb_group_cnt[bid2gid(i)] -= 1;
            if (i >= k) {
              for (int j = 0; j < l; j++) {
                fb_group_cnt[j] -= 1;
              }
            }
          }
        }
        num_of_failed_data_or_global = 0;
      }

      if (iter_cnt > 0 && num_of_failed_blocks > 0) {
        bool if_decodable = true;
        if_decodable = check_if_decodable(failure_idxs);
        if (if_decodable) {  // if decodable, repair in one go
          RepairPlan plan;
          plan.local_or_column = false;
          for (int i = 0; i < k + g + l; i++) {
            if (failed_map[i]) {
              plan.failure_idxs.push_back(i);
            }
          }

          help_blocks_for_multi_blocks_repair_oneoff(plan.failure_idxs, plan.help_blocks);
          plans.push_back(plan);

          // update 
          for (int i = 0; i < k + g + l; i++) {
            if (failed_map[i]) {
              failed_map[i] = 0;
              num_of_failed_blocks -= 1;
              fb_group_cnt[bid2gid(i)] -= 1;
              if (i >= k && i < k + g) {
                for (int j = 0; j < l; j++) {
                  fb_group_cnt[j] -= 1;
                }
              }
            }
          }
          num_of_failed_data_or_global = 0;
        } else {
          std::cout << "Undecodable!!!" << std::endl;
          return false;
        }
      }
      iter_cnt++;
    }
	}
  return true;
}

bool Uni_Cau_LRC::check_if_decodable(std::vector<int> failure_idxs)
{
  int failed_num = (int)failure_idxs.size();
  std::unordered_map<int, int> b2g;
  std::vector<int> group_fd_cnt;
  std::vector<int> group_fgp_cnt;
  std::vector<int> group_slp_cnt;
  std::vector<bool> group_pure_flag;
  int sgp_cnt = g;
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k + g - i * r);
    for (int j = 0; j < group_size; j++) {
      b2g.insert(std::make_pair(idx, i));
      idx++;
    }
    if (idx <= k || idx - group_size >= k) {
      group_pure_flag.push_back(true);
    } else {
      group_pure_flag.push_back(false);
    }
    b2g.insert(std::make_pair(k + g + i, i));
    group_fd_cnt.push_back(0);
    group_fgp_cnt.push_back(0);
    group_slp_cnt.push_back(1);
  }

  for (int i = 0; i < failed_num; i++) {
    int block_id = failure_idxs[i];
    if (block_id < k) {
      group_fd_cnt[b2g[block_id]] += 1;
    } else if (block_id < k + g && block_id >= k) {
      group_fgp_cnt[b2g[block_id]] += 1;
      sgp_cnt -= 1;
    } else {
      group_slp_cnt[block_id - k - g] -= 1;
    }
  }

  for (int i = 0; i < l; i++) {
    if (group_slp_cnt[i] && group_pure_flag[i]) {
      if (group_slp_cnt[i] <= group_fd_cnt[i]) {
        group_fd_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
      }
      if (group_slp_cnt[i] && group_slp_cnt[i] == group_fgp_cnt[i]) {
        group_fgp_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
        sgp_cnt += 1;
      }
    } else if (group_slp_cnt[i] && !group_pure_flag[i]) {
      if (group_fd_cnt[i] == 1 && !group_fgp_cnt[i]) {
        group_fd_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
      } else if (group_fgp_cnt[i] == 1 && !group_fd_cnt[i]) {
        group_fgp_cnt[i] -= group_slp_cnt[i];
        group_slp_cnt[i] = 0;
        sgp_cnt += 1;
      }
    }
  }
  for (int i = 0; i < l; i++) {
    if (sgp_cnt >= group_fd_cnt[i]) {
      sgp_cnt -= group_fd_cnt[i];
      group_fd_cnt[i] = 0;
    } else {
      return false;
    }
  }
  return true;
}

void Uni_Cau_LRC::make_encoding_matrix(int *final_matrix)
{
  int *matrix = cauchy_good_general_coding_matrix(k, g + 1, w);

  bzero(final_matrix, sizeof(int) * k * (g + l));

  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[i * k + j] = matrix[i * k + j];
    }
  }

  std::vector<int> l_matrix(l * k, 0);
  int d_idx = 0;
  int l_idx = 0;
  for (int i = 0; i < l && d_idx < k; i++) {
    int group_size = std::min(r, k + g - i * r);
    for (int j = 0; j < group_size && d_idx < k; j++) {
      l_matrix[i * k + d_idx] = matrix[g * k + d_idx];
      d_idx++;
    }
    l_idx = i;
  }

  // print_matrix(l_matrix.data(), l, k, "l_matrix_before_xor");

  int g_idx = 0;
  for (int i = l_idx; i < l; i++) {
    int sub_g_num = -1;
    int group_size = std::min(r, k + g - i * r);
    if (group_size < r) { // must be the last group
      sub_g_num = g - g_idx;
    } else {
      sub_g_num = (i + 1) * r - (k + g_idx);
    }
    std::vector<int> sub_g_matrix(sub_g_num * k, 0);
    for (int j = 0; j < sub_g_num; j++) {
      for (int jj = 0; jj < k; jj++) {
        sub_g_matrix[j * k + jj] = matrix[g_idx * k + jj];
      }
      g_idx++;
    }
    for (int j = 0; j < sub_g_num; j++) {
      galois_region_xor((char *)&sub_g_matrix[j * k], (char *)&l_matrix[i * k], 4 * k);
    }
  }

  // print_matrix(l_matrix.data(), l, k, "l_matrix_after_xor");

  int idx = g * k;
  for (int i = 0; i < l; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[idx + i * k + j] = l_matrix[i * k + j];
    }
  }

  // print_matrix(final_matrix, g + l, k, "final_matrix");

  free(matrix);
}

void Uni_Cau_LRC::make_encoding_matrix_v2(int *final_matrix)
{
  int *matrix = cauchy_good_general_coding_matrix(k, g + 1, w);

  bzero(final_matrix, sizeof(int) * k * (g + l));

  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[i * k + j] = matrix[i * k + j];
    }
  }

  std::vector<int> l_matrix(l * (k + g), 0);
  std::vector<int> d_g_matrix((k + g) * k, 0);
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k + g - i * r);
    for (int j = 0; j < group_size; j++) {
      if (idx < k) {
        l_matrix[i * (k + g) + idx] = matrix[g * k + idx];
      } else {
        l_matrix[i * (k + g) + idx] = 1;
      }
      idx++;
    }
  }
  for (int i = 0; i < k; i++) {
    d_g_matrix[i * k + i] = 1;
  }
  idx = k * k;
  for (int i = 0; i < g; i++) {
    for (int j = 0; j < k; j++) {
      d_g_matrix[idx + i * k + j] = matrix[i * k + j];
    }
  }

  // print_matrix(l_matrix.data(), l, k + g, "l_matrix");
  // print_matrix(d_g_matrix.data(), k + g, k, "d_g_matrix");

  int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(),
                                             l, k + g, k + g, k, w);

  idx = g * k;
  for (int i = 0; i < l; i++) {
    for (int j = 0; j < k; j++) {
      final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
    }
  }

  // print_matrix(final_matrix, g + l, k, "final_matrix");

  free(matrix);
  free(mix_matrix);
}

void Uni_Cau_LRC::make_group_matrix(int *group_matrix, int group_id)
{
  int *matrix = cauchy_good_general_coding_matrix(k, g + 1, 8);
  int idx = 0;
  for (int i = 0; i < l; i++) {
    int group_size = std::min(r, k + g - i * r);
    for (int j = 0; j < group_size; j++) {
      if (i == group_id) {
        if (idx < k) {
          group_matrix[j] = matrix[g * k + idx];
        } else {
          group_matrix[j] = 1;
        }
      }
      idx++;
    }
  }
}

bool Uni_Cau_LRC::check_parameters()
{
  if (r * (l - 1) < k + g || !k || !l || !g)
    return false;
  return true;
}

int Uni_Cau_LRC::bid2gid(int block_id)
{
  int group_id = -1;
  if (block_id < k + g) {
    group_id = block_id / r;
  } else {
    group_id = block_id - k - g;
  }
  return group_id;
}

int Uni_Cau_LRC::idxingroup(int block_id)
{
  if (block_id < k + g) {
    return block_id % r;
  } else {
    if (block_id - k - g < l - 1) {
      return r;
    } else {
      return (k + g) % r == 0 ? r : (k + g) % r;
    }
  }
}

int Uni_Cau_LRC::get_group_size(int group_id, int& min_idx)
{
  min_idx = group_id * r;
  if (group_id < l - 1) {
    return r;
  } else {
    return (k + g) % r == 0 ? r : (k + g) % r;
  }
}

void Uni_Cau_LRC::grouping_information(std::vector<std::vector<int>>& groups)
{
  int idx = 0;
  for (int i = 0; i < l; i++) {
    std::vector<int> local_group;
    int group_size = std::min(r, k + g - i * r);
    for (int j = 0; j < group_size; j++) {
      local_group.push_back(idx++);
    }
    local_group.push_back(k + g + i);
    groups.push_back(local_group);
  }
}

void Uni_Cau_LRC::partition_optimal()
{
  std::vector<std::vector<int>> stripe_groups;
  grouping_information(stripe_groups);

  for (int i = 0; i < l; i++) {
    std::vector<int> local_group = stripe_groups[i];
    int group_size = int(local_group.size());
    // every g + 1 blocks a partition
    for (int j = 0; j < group_size; j += g + 1) {
      std::vector<int> partition;
      for (int ii = j; ii < j + g + 1 && ii < group_size; ii++) {
        partition.push_back(local_group[ii]);
      }
      partition_plan.push_back(partition);
    }
  }
}

std::string Uni_Cau_LRC::self_information()
{
	return "Uniform_Cauchy_LRC(" + std::to_string(k) + "," + std::to_string(l) + \
         "," + std::to_string(g) + ")";
}