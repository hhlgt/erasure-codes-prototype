#pragma once

#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "utils.h"

namespace ECProject
{
	enum ECFAMILY
  {
    RSCodes,
    LRCs,
    PCs
  };

  enum ECTYPE
  {
    RS,
		ERS,
    AZURE_LRC,
    AZURE_LRC_1,
    OPTIMAL_LRC,
    OPTIMAL_CAUCHY_LRC,
    UNIFORM_CAUCHY_LRC,
    PC,
    Hierachical_PC,
    HV_PC
  };

	enum PlacementRule
	{
		FLAT,
		RANDOM,
		OPTIMAL
	};

	struct CodingParameters
  {
    int k;
    int m;
    int l;
    int g;
    int k1;
    int m1;
    int k2;
    int m2;
		int x;
		int seri_num = 0;
		bool local_or_column = false;
  };

	struct RepairPlan
	{
		bool local_or_column = false;
		std::vector<int> failure_idxs;
		std::vector<std::vector<int>> help_blocks;
	};
	
	class ErasureCode
	{
	public:
		int k;		 /* number of data blocks */
		int m;		 /* number of parity blocks */
		int w = 8; /* word size for encoding */
		PlacementRule placement_rule = FLAT;
		bool local_or_column = false;
		std::vector<std::vector<int>> partition_plan;

		ErasureCode()
		{
			k = 6;
			m = 3;
			w = 8;
		}
		ErasureCode(int k, int m) : k(k), m(m) { w = 8; }
		virtual ~ErasureCode() {}

		virtual void init_coding_parameters(CodingParameters cp);
		virtual void get_coding_parameters(CodingParameters& cp);

		virtual void encode(char **data_ptrs, char **coding_ptrs, int block_size) = 0;
		virtual void decode(char **data_ptrs, char **coding_ptrs, int blocksize,
												int *erasures, int failed_num) = 0;
		virtual bool check_if_decodable(std::vector<int> failure_idxs) = 0;
		virtual void make_encoding_matrix(int *final_matrix) = 0;
		virtual void encode_partial_blocks_for_encoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs) = 0;
		virtual void encode_partial_blocks_for_decoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs) = 0;

		void print_matrix(int *matrix, int rows, int cols, std::string msg);
		void get_full_matrix(int *matrix, int kk);
		void make_submatrix_by_rows(int cols, int *matrix, int *new_matrix,
																std::vector<int> block_idxs);
		void make_submatrix_by_cols(int cols, int rows, int *matrix, int *new_matrix,
																std::vector<int> blocks_idxs);
		void perform_addition(char **data_ptrs, char **coding_ptrs,
													int block_size, int block_num, int parity_num);
		void encode_partial_blocks_for_encoding_(int k_, int *full_matrix,
																						 char **data_ptrs, char **coding_ptrs,
																						 int block_size,
																						 std::vector<int> data_idxs,
																						 std::vector<int> parity_idxs);
		void encode_partial_blocks_for_decoding_(int k_, int *full_matrix,
																						 char **data_ptrs, char **coding_ptrs,
																						 int block_size,
																						 std::vector<int> local_survivor_idxs,
																						 std::vector<int> survivor_idxs,
																						 std::vector<int> failure_idxs);

		// partition stragtegy, subject to single-region fault tolerance
		virtual void partition_flat();
		virtual void partition_random() = 0;
		virtual void partition_optimal() = 0;
		void generate_partition();
		void print_info(std::vector<std::vector<int>> info, std::string info_str);

		virtual std::string self_information() = 0;

		virtual bool generate_repair_plan(std::vector<int> failure_idxs,
																			std::vector<RepairPlan>& plans) = 0;
		
		virtual void transfer_block_idxs(std::vector<int>& block_idxs) {};

	};
}