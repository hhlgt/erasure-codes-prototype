#pragma once

#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "erasure_code.h"

namespace ECProject
{
	class RSCode : public ErasureCode
	{
	public:
		RSCode() {}
		RSCode(int k, int m) : ErasureCode(k, m) {}
		~RSCode() override {}

		void encode(char **data_ptrs, char **coding_ptrs, int block_size) override;
		void decode(char **data_ptrs, char **coding_ptrs, int block_size,
								int *erasures, int failed_num) override;
		bool check_if_decodable(std::vector<int> failure_idxs) override;

		void make_encoding_matrix(int *final_matrix) override;
		void encode_partial_blocks_for_encoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs) override;
		void encode_partial_blocks_for_decoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs) override;

		void partition_random() override;
		void partition_optimal() override;

		std::string self_information() override;

		void help_blocks_for_single_block_repair_oneoff(
						int failure_idx, std::vector<std::vector<int>>& help_blocks);
		void help_blocks_for_multi_blocks_repair_oneoff(
						std::vector<int> failure_idxs,
						std::vector<std::vector<int>>& help_blocks);
		
		bool generate_repair_plan(std::vector<int> failure_idxs,
															std::vector<RepairPlan>& plans) override;
	};

	class EnlargedRSCode : public RSCode
	{
	public:
		int x = 2;
		int seri_num = 1;

		EnlargedRSCode() {}
		EnlargedRSCode(int k, int m)
				: RSCode(k, m) {}
		~EnlargedRSCode() override {}

		void init_coding_parameters(CodingParameters cp) override;
		void make_encoding_matrix(int *final_matrix) override;
		std::string self_information() override;
	};
}
