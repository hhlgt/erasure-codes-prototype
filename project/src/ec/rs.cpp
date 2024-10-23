#include "rs.h"

using namespace ECProject;

void RSCode::make_encoding_matrix(int *final_matrix)
{
	int *matrix = reed_sol_vandermonde_coding_matrix(k, m, w);

	bzero(final_matrix, sizeof(int) * k * m);

	for (int i = 0; i < m; i++) {
		for (int j = 0; j < k; j++) {
			final_matrix[i * k + j] = matrix[i * k + j];
		}
	}

	free(matrix);
}

void RSCode::encode(char **data_ptrs, char **coding_ptrs, int block_size)
{
	std::vector<int> rs_matrix(k * m, 0);
	make_encoding_matrix(rs_matrix.data());
	jerasure_matrix_encode(k, m, w, rs_matrix.data(), data_ptrs, coding_ptrs, block_size);
}

void RSCode::decode(char **data_ptrs, char **coding_ptrs, int block_size,
										int *erasures, int failed_num)
{
	if (failed_num > m) {
		std::cout << "[Decode] Undecodable!" << std::endl;
		return;
	}
	int *rs_matrix = reed_sol_vandermonde_coding_matrix(k, m, w);
	int ret = 0;
	ret = jerasure_matrix_decode(k, m, w, rs_matrix, failed_num, erasures,
															 data_ptrs, coding_ptrs, block_size);
	if (ret == -1) {
		std::cout << "[Decode] Failed!" << std::endl;
		return;
	}
}

void RSCode::encode_partial_blocks_for_encoding(
		char **data_ptrs, char **coding_ptrs, int block_size,
		std::vector<int> data_idxs, std::vector<int> parity_idxs)
{
	std::vector<int> rs_matrix((k + m) * k, 0);
	get_full_matrix(rs_matrix.data(), k);
	make_encoding_matrix(&(rs_matrix.data())[k * k]);
	encode_partial_blocks_for_encoding_(k, rs_matrix.data(), data_ptrs, coding_ptrs,
																			block_size, data_idxs, parity_idxs);
}

void RSCode::encode_partial_blocks_for_decoding(
		char **data_ptrs, char **coding_ptrs, int block_size,
		std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
		std::vector<int> failure_idxs)
{
	std::vector<int> rs_matrix((k + m) * k, 0);
	get_full_matrix(rs_matrix.data(), k);
	make_encoding_matrix(&(rs_matrix.data())[k * k]);
	encode_partial_blocks_for_decoding_(k, rs_matrix.data(), data_ptrs, coding_ptrs,
																			block_size, local_survivor_idxs,
																			survivor_idxs, failure_idxs);
}

bool RSCode::check_if_decodable(std::vector<int> failure_idxs)
{
	int failed_num = (int)failure_idxs.size();
	if (m >= failed_num) {
		return true;
	} else {
		return false;
	}
}

void RSCode::partition_random()
{
	int n = k + m;
	std::vector<int> blocks;
	for (int i = 0; i < n; i++) {
		blocks.push_back(i);
	}

	int cnt = 0;
	int cnt_left = n;
	while (cnt < n) {
		// at least subject to single-region fault tolerance
		int random_partition_size = random_range(1, m);
		int partition_size = std::min(random_partition_size, n - cnt);
		std::vector<int> partition;
		for (int i = 0; i < partition_size; i++, cnt++) {
			int ran_idx = random_index(n - cnt);
			int block_idx = blocks[ran_idx];
			partition.push_back(block_idx);
			auto it = std::find(blocks.begin(), blocks.end(), block_idx);
			blocks.erase(it);
		}
		partition_plan.push_back(partition);
	}
}

void RSCode::partition_optimal()
{
	int n = k + m;
	int cnt = 0;
	while (cnt < n) {
		// every m blocks in a partition
		int partition_size = std::min(m, n - cnt);
		std::vector<int> partition;
		for (int i = 0; i < partition_size; i++, cnt++) {
			partition.push_back(cnt);
		}
		partition_plan.push_back(partition);
	}
}

std::string RSCode::self_information()
{
	return "RS(" + std::to_string(k) + "," + std::to_string(m) + ")";
}

void RSCode::help_blocks_for_single_block_repair_oneoff(
								int failure_idx, std::vector<std::vector<int>>& help_blocks)
{
	int parition_num = (int)partition_plan.size();
	if (!parition_num) {
    return;
  }

	int main_parition_idx = -1;
	std::vector<std::pair<int, int>> partition_idx_to_num;
	for (int i = 0; i < parition_num; i++) {
		auto it = std::find(partition_plan[i].begin(), partition_plan[i].end(),
												failure_idx);
		if (it != partition_plan[i].end()) {
			main_parition_idx = i;
		} else {
			int partition_size = (int)partition_plan[i].size();
			partition_idx_to_num.push_back(std::make_pair(i, partition_size));
		}
	}
	std::sort(partition_idx_to_num.begin(), partition_idx_to_num.end(),
						cmp_descending);

	int cnt = 0;
	std::vector<int> main_help_block;
	for (auto idx : partition_plan[main_parition_idx]) {
		if (idx != failure_idx) {
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
			if (cnt < k) {
				help_block.push_back(idx);
				cnt++;
			} else {
				break;
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

void RSCode::help_blocks_for_multi_blocks_repair_oneoff(
							std::vector<int> failure_idxs, std::vector<std::vector<int>>& help_blocks)
{
	int parition_num = (int)partition_plan.size();
	if (!parition_num) {
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
	for (auto& pair : main_partition_idx_to_num) {
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

bool RSCode::generate_repair_plan(std::vector<int> failure_idxs,
																	std::vector<RepairPlan>& plans)
{
	int failed_num = (int)failure_idxs.size();
	RepairPlan plan;
	for (auto idx : failure_idxs) {
		plan.failure_idxs.push_back(idx);
	}
	if (failed_num == 1) {
		help_blocks_for_single_block_repair_oneoff(failure_idxs[0], plan.help_blocks);
	} else {
		help_blocks_for_multi_blocks_repair_oneoff(failure_idxs, plan.help_blocks);
	}
	plans.push_back(plan);
	return true;
}

void EnlargedRSCode::init_coding_parameters(CodingParameters cp)
{
	k = cp.k;
	m = cp.m;
	x = cp.x;
	seri_num = cp.seri_num;
}

void EnlargedRSCode::make_encoding_matrix(int *final_matrix)
{
	if (seri_num >= x)
	{
		std::cout << "Invalid argurments!" << std::endl;
		return;
	}
	int *rs_matrix = reed_sol_vandermonde_coding_matrix(x * k, m, w);
	int index = 0;
	for (int i = 0; i < m; i++)
	{
		memcpy(&final_matrix[index], &rs_matrix[i * k * x + seri_num * k], k * sizeof(int));
		index += k;
	}
	free(rs_matrix);
}

std::string EnlargedRSCode::self_information()
{
	return "EnlargedRS(" + std::to_string(k) + "," + std::to_string(m) +
				 "|" + std::to_string(x) + "," + std::to_string(seri_num) + ")";
}
