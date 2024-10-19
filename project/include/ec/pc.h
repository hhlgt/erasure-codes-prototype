#pragma once

#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "erasure_code.h"
#include "rs.h"

namespace ECProject
{
	class ProductCode : public ErasureCode
	{
	public:
		RSCode row_code;
		RSCode col_code;
		int k1;
		int m1;
		int k2;
		int m2;

		ProductCode() {}
		ProductCode(int k1, int m1, int k2, int m2)
			: ErasureCode(k1 * k2, (k1 + m1) * (k2 + m2) - k1 * k2),
				row_code(k1, m1), col_code(k2, m2), k1(k1), m1(m1), k2(k2), m2(m2) {}
		~ProductCode() override {}

		void init_coding_parameters(CodingParameters cp) override;
		void get_coding_parameters(CodingParameters& cp) override;

		void encode(char **data_ptrs, char **coding_ptrs, int block_size) override;
		void decode(char **data_ptrs, char **coding_ptrs, int block_size,
								int *erasures, int failed_num) override;
		bool check_if_decodable(std::vector<int> failure_idxs) override;

		void make_encoding_matrix(int *final_matrix) override {}
		void encode_partial_blocks_for_encoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs) override;
		void encode_partial_blocks_for_decoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs) override;
		
		int rowcol2bid(int row, int col);
		void bid2rowcol(int block_id, int &row, int &col);
		virtual int oldbid2newbid_for_merge(
				int old_block_id, int x, int seri_num, bool isvertical);

		void partition_flat() override;
		void partition_random() override;
		void partition_optimal() override;

		std::string self_information() override;

		bool generate_repair_plan(std::vector<int> failure_idxs,
															std::vector<RepairPlan>& plans) override;
	};

	/* Hierachical-aware Product Codes with enlarged encoding matrix */
	class HPC : public ProductCode
	{
	public:
		EnlargedRSCode e_row_code;
		EnlargedRSCode e_col_code;
		bool isvertical = true;
	
		HPC() {}
		HPC(int k1, int m1, int k2, int m2)
			: ProductCode(k1, m1, k2, m2), e_row_code(k1, m1),
				e_col_code(k2, m2) {}
		~HPC() override {}

		void init_coding_parameters(CodingParameters cp) override;

		void encode(char **data_ptrs, char **coding_ptrs, int block_size) override;
		void decode(char **data_ptrs, char **coding_ptrs, int block_size,
								int *erasures, int failed_num) override;
		
		void encode_partial_blocks_for_encoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs) override;
		void encode_partial_blocks_for_decoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs) override;

		int oldbid2newbid_for_merge(
				int old_block_id, int x, int seri_num, bool isvertical) override;

		std::string self_information() override;
	};

	/* ProductCode without global parity blocks */
	class HVPC : public ProductCode
	{
	public:
		HVPC() {}
		HVPC(int k1, int m1, int k2, int m2)
			: ProductCode(k1, m1, k2, m2)
		{
			m = k1 * m2 + k2 * m1;
		}
		~HVPC() override {}

		void init_coding_parameters(CodingParameters cp) override;
		void encode(char **data_ptrs, char **coding_ptrs, int block_size) override;
		void decode(char **data_ptrs, char **coding_ptrs, int block_size,
								int *erasures, int failed_num) override;
		bool check_if_decodable(std::vector<int> failure_idxs) override;

		void partition_random() override;
		void partition_optimal() override;

		std::string self_information() override;
		
		bool generate_repair_plan(std::vector<int> failure_idxs,
															std::vector<RepairPlan>& plans) override;
	};
}
