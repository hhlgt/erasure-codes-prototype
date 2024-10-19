#include "erasure_code.h"

using namespace ECProject;

void ErasureCode::init_coding_parameters(CodingParameters cp)
{
	k = cp.k;
	m = cp.m;
	local_or_column = cp.local_or_column;
}

void ErasureCode::get_coding_parameters(CodingParameters& cp)
{
	cp.k = k;
	cp.m = m;
	cp.local_or_column = local_or_column;
}

void ErasureCode::print_matrix(int *matrix, int rows, int cols, std::string msg)
{
	std::cout << msg << ":" << std::endl;
	for (int i = 0; i < rows; i++) {
		for (int j = 0; j < cols; j++) {
			std::cout << matrix[i * cols + j] << " ";
		}
		std::cout << std::endl;
	}
}

void ErasureCode::get_full_matrix(int *matrix, int kk)
{
	for (int i = 0; i < kk; i++) {
		matrix[i * kk + i] = 1;
	}
}

void ErasureCode::make_submatrix_by_rows(int cols, int *matrix, int *new_matrix,
										 										 std::vector<int> block_idxs)
{
	int i = 0;
	for (auto it = block_idxs.begin(); it != block_idxs.end(); it++) {
		int j = *it;
		memcpy(&new_matrix[i * cols], &matrix[j * cols], cols * sizeof(int));
		i++;
	}
}

void ErasureCode::make_submatrix_by_cols(int cols, int rows,
																				 int *matrix, int *new_matrix,
																				 std::vector<int> block_idxs)
{
	int block_num = int(block_idxs.size());
	int i = 0;
	for (auto it = block_idxs.begin(); it != block_idxs.end(); it++) {
		int j = *it;
		for (int u = 0; u < rows; u++) {
			new_matrix[u * block_num + i] = matrix[u * cols + j];
		}
		i++;
	}
}

/*
	we assume the blocks is organized as followed:
	data_ptrs = [B11, ..., Bp1, B12, ..., Bp2, ........., B1n, ..., Bpn]
	coding_ptrs = [P1, ..., Pn]
	block_num = p * n, parity_num = p
	then Pi = Bi1 + Bi2 + ... + Bin, 1 <= i <= p
*/
void ErasureCode::perform_addition(char **data_ptrs, char **coding_ptrs,
								   								 int block_size, int block_num, int parity_num)
{
  if (block_num % parity_num != 0) {
    printf("invalid! %d mod %d != 0\n", block_num, parity_num);
    return;
  }
  int block_num_per_parity = block_num / parity_num;

  std::vector<char *> t_data(block_num);
  char **data = (char **)t_data.data();
	int cnt = 0;
	for (int i = 0; i < parity_num; i++) {
		for (int j = 0; j < block_num_per_parity; j++) {
			data[cnt++] = data_ptrs[j * parity_num + i];
		}
	}

  for (int i = 0; i < parity_num; i++) {
    std::vector<int> new_matrix(1 * block_num_per_parity, 1);
    jerasure_matrix_encode(block_num_per_parity, 1, w, new_matrix.data(),
													 &data[i * block_num_per_parity], &coding_ptrs[i],
													 block_size);
  }
}

// any parity_idx >= k
void ErasureCode::encode_partial_blocks_for_encoding_(int k_,
		int *full_matrix, char **data_ptrs, char **coding_ptrs, int block_size,
		std::vector<int> data_idxs, std::vector<int> parity_idxs)
{
	int block_num = int(data_idxs.size());
	int parity_num = int(parity_idxs.size());
	std::vector<int> matrix(parity_num * k_, 0);
	make_submatrix_by_rows(k_, full_matrix, matrix.data(), parity_idxs);

	std::vector<int> new_matrix(parity_num * block_num, 1);
	make_submatrix_by_cols(k_, parity_num, matrix.data(), new_matrix.data(), data_idxs);

  jerasure_matrix_encode(block_num, parity_num, w, new_matrix.data(), data_ptrs,
												 coding_ptrs, block_size);
}

void ErasureCode::encode_partial_blocks_for_decoding_(int k_,
		int *full_matrix, char **data_ptrs, char **coding_ptrs, int block_size,
		std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
		std::vector<int> failure_idxs)
{
	int local_survivors_num = int(local_survivor_idxs.size());
	int failures_num = int(failure_idxs.size());
	std::vector<int> failures_matrix(failures_num * k_, 0);
	std::vector<int> survivors_matrix(k_ * k_, 0);
	make_submatrix_by_rows(k_, full_matrix, failures_matrix.data(), failure_idxs);
	make_submatrix_by_rows(k_, full_matrix, survivors_matrix.data(), survivor_idxs);
	// print_matrix(failures_matrix.data(), failures_num, k_, "failures_matrix");
	// print_matrix(survivors_matrix.data(), k_, k_, "survivors_matrix");

	std::vector<int> inverse_matrix(k_ * k_, 0);
	jerasure_invert_matrix(survivors_matrix.data(), inverse_matrix.data(), k_, w);
	// print_matrix(inverse_matrix.data(), k_, k_, "inverse_matrix");
	
	int *decoding_matrix = jerasure_matrix_multiply(failures_matrix.data(),
																									inverse_matrix.data(),
																									failures_num, k_, k_, k_, w);
	std::vector<int> encoding_matrix(failures_num * local_survivors_num, 0);
	int i = 0;
	for (auto it2 = local_survivor_idxs.begin(); it2 != local_survivor_idxs.end(); it2++, i++) {
		int idx = 0;
		for (auto it3 = survivor_idxs.begin(); it3 != survivor_idxs.end(); it3++, idx++) {
			if(*it2 == *it3)
				break;
		}

		for (int u = 0; u < failures_num; u++) {
			encoding_matrix[u * local_survivors_num + i] = decoding_matrix[u * k_ + idx];
		}
	}
	jerasure_matrix_encode(local_survivors_num, failures_num, w, encoding_matrix.data(),
												 data_ptrs, coding_ptrs, block_size);
	free(decoding_matrix);
}

void ErasureCode::partition_flat()
{
	int n = k + m;
  for(int i = 0; i < n; i++) {
    partition_plan.push_back({i});
  }
}

void ErasureCode::generate_partition()
{
	partition_plan.clear();
	if (placement_rule == FLAT) {
		partition_flat();
	} else if (placement_rule == RANDOM) {
		partition_random();
	} else if (placement_rule == OPTIMAL) {
		partition_optimal();
	} 
}

void ErasureCode::print_info(std::vector<std::vector<int>> info, std::string info_str)
{
	if (info_str == "placement") {
		std::string placement_type = "_flat";
		if (placement_rule == RANDOM) {
			placement_type = "_random";
		} else if (placement_rule == OPTIMAL) {
			placement_type = "_optimal";
		}
		info_str += placement_type;
	}
	
	std::cout << info_str << " result:\n";
	int cnt = 0;
	for (auto& vec : info) {
		std::cout << cnt++ << ": ";
		for (int ele : vec) {
			std::cout << ele << " ";
		}
		std::cout << "\n";
	}
}
