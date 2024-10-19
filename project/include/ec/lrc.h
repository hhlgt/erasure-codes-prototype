#pragma once

#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "erasure_code.h"

namespace ECProject
{
	class LocallyRepairableCode : public ErasureCode
	{
	public:
		int l;	/* number of local parity blocks */
		int g;	/* number of global parity blocks */
		int r;	/* regular group size */

		LocallyRepairableCode() {}
		LocallyRepairableCode(int k, int l, int g)
			: ErasureCode(k, l + g), l(l), g(g)
		{
			my_assert(l > 0);
  		r = (k + l - 1) / l;
		}
		~LocallyRepairableCode() override {}

		void init_coding_parameters(CodingParameters cp) override;
		void get_coding_parameters(CodingParameters& cp) override;

		void encode(char **data_ptrs, char **coding_ptrs, int block_size) override;
		void decode(char **data_ptrs, char **coding_ptrs, int block_size,
								int *erasures, int failed_num) override;
		void decode_global(char **data_ptrs, char **coding_ptrs, int block_size,
										int *erasures, int failed_num);
		void decode_local(char **data_ptrs, char **coding_ptrs, int block_size,
										int *erasures, int failed_num, int group_id);
		void encode_partial_blocks_for_encoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs) override;
		void encode_partial_blocks_for_decoding(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs) override;
		void encode_partial_blocks_for_encoding_global(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs);
		void encode_partial_blocks_for_decoding_global(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs);
		virtual void encode_partial_blocks_for_encoding_local(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs);
		virtual void encode_partial_blocks_for_decoding_local(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs);

		// check if decodable
		bool check_if_decodable(std::vector<int> failure_idxs) override { return true; }
		// full encoding matrix, with demensions as (g + l) × k
		void make_encoding_matrix(int *final_matrix) override {}
		// encoding matrix for a single local parity, with demensions as 1 × group_size
		virtual void make_group_matrix(int *group_matrix, int group_id) {}
		// check the validation of coding parameters
		virtual bool check_parameters() = 0;
		// get group_id via block_id
		virtual int bid2gid(int block_id) = 0;
		// get encoding index in a group via block_id
		virtual int idxingroup(int block_id) = 0;
		// get the size of the group-id-th group
		virtual int get_group_size(int group_id, int& min_idx) { return 0; } 

		virtual void grouping_information(std::vector<std::vector<int>>& groups) = 0;
		void partition_random() override;
		void partition_optimal() override {}

		std::string self_information() override { return ""; }

		virtual void help_blocks_for_single_block_repair_oneoff(
										int failure_idx, std::vector<std::vector<int>>& help_blocks);
		void help_blocks_for_multi_blocks_repair_oneoff(	// repair with global parity blocks
						std::vector<int> failure_idxs,
						std::vector<std::vector<int>>& help_blocks);
		bool generate_repair_plan(std::vector<int> failure_idxs,
															std::vector<RepairPlan>& plans) override;
	};
	
	/* Microsoft's Azure LRC */
	class Azu_LRC : public LocallyRepairableCode
	{
	public:
		Azu_LRC() {}
		Azu_LRC(int k, int l, int g) : LocallyRepairableCode(k, l, g)
		{
			my_assert(l > 0);
  		r = (k + l - 1) / l;
		}
		~Azu_LRC() override {}

		bool check_if_decodable(std::vector<int> failure_idxs) override;
		void make_encoding_matrix(int *final_matrix) override;
		void make_group_matrix(int *group_matrix, int group_id) override;
		bool check_parameters() override;
		int bid2gid(int block_id) override;
		int idxingroup(int block_id) override;
		int get_group_size(int group_id, int& min_idx) override;

		void grouping_information(std::vector<std::vector<int>>& groups) override;
		void partition_optimal() override;
		void partition_sub_optimal();

		std::string self_information() override;
	};

	/* Microsoft's Azure LRC + 1 */
	class Azu_LRC_1 : public LocallyRepairableCode
	{
	public:
		Azu_LRC_1() {}
		Azu_LRC_1(int k, int l, int g) : LocallyRepairableCode(k, l, g)
		{
			my_assert(l > 1);
  		r = (k + l - 2) / (l - 1);
		}
		~Azu_LRC_1() override {}

		bool check_if_decodable(std::vector<int> failure_idxs) override;
		void make_encoding_matrix(int *final_matrix) override;
		void make_group_matrix(int *group_matrix, int group_id) override;
		bool check_parameters() override;
		int bid2gid(int block_id) override;
		int idxingroup(int block_id) override;
		int get_group_size(int group_id, int& min_idx) override;

		void grouping_information(std::vector<std::vector<int>>& groups) override;
		void partition_optimal() override;

		std::string self_information() override;
	};

	/* Optimal LRC */
	class Opt_LRC : public LocallyRepairableCode
	{
	public:
		Opt_LRC() {}
		Opt_LRC(int k, int l, int g) : LocallyRepairableCode(k, l, g)
		{
			my_assert(l > 0);
  		r = (k + g + l - 1) / l;
		}
		~Opt_LRC() override {}

		bool check_if_decodable(std::vector<int> failure_idxs) override;
		void make_encoding_matrix(int *final_matrix) override;
		void make_group_matrix(int *group_matrix, int group_id) override;
		bool check_parameters() override;
		int bid2gid(int block_id) override;
		int idxingroup(int block_id) override;
		int get_group_size(int group_id, int& min_idx) override;

		void grouping_information(std::vector<std::vector<int>>& groups) override;
		void partition_optimal() override;

		std::string self_information() override;
	};

	/* Optimal Cauchy LRC [FAST'23, Google] */
	class Opt_Cau_LRC : public LocallyRepairableCode
	{
	public:
		int surviving_group_id;

		Opt_Cau_LRC() {}
		Opt_Cau_LRC(int k, int l, int g) : LocallyRepairableCode(k, l, g)
		{
			my_assert(l > 0);
  		r = (k + l - 1) / l;
		}
		~Opt_Cau_LRC() override {}

		void encode_partial_blocks_for_encoding_local(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> data_idxs, std::vector<int> parity_idxs) override;
		void encode_partial_blocks_for_decoding_local(
				char **data_ptrs, char **coding_ptrs, int block_size,
				std::vector<int> local_survivor_idxs, std::vector<int> survivor_idxs,
				std::vector<int> failure_idxs) override;

		bool check_if_decodable(std::vector<int> failure_idxs) override;
		void make_encoding_matrix(int *final_matrix) override;
		void make_encoding_matrix_v2(int *final_matrix);
		void make_group_matrix(int *group_matrix, int group_id) override;
		bool check_parameters() override;
		int bid2gid(int block_id) override;
		int idxingroup(int block_id) override;
		int get_group_size(int group_id, int& min_idx) override;

		void grouping_information(std::vector<std::vector<int>>& groups) override;
		void partition_optimal() override;

		std::string self_information() override;

		void help_blocks_for_single_block_repair_oneoff(
						int failure_idx, std::vector<std::vector<int>>& help_blocks) override;
		bool generate_repair_plan(std::vector<int> failure_idxs,
															std::vector<RepairPlan>& plans) override;
	};

	/* Uniform Cauchy LRC [FAST'23, Google] */
	class Uni_Cau_LRC : public LocallyRepairableCode
	{
	public:
		Uni_Cau_LRC() {}
		Uni_Cau_LRC(int k, int l, int g) : LocallyRepairableCode(k, l, g)
		{
			my_assert(l > 0);
  		r = (k + g + l - 1) / l;
		}
		~Uni_Cau_LRC() override {}

		bool check_if_decodable(std::vector<int> failure_idxs) override;
		void make_encoding_matrix(int *final_matrix) override;
		void make_encoding_matrix_v2(int *final_matrix);
		void make_group_matrix(int *group_matrix, int group_id) override;
		bool check_parameters() override;
		int bid2gid(int block_id) override;
		int idxingroup(int block_id) override;
		int get_group_size(int group_id, int& min_idx) override;

		void grouping_information(std::vector<std::vector<int>>& groups) override;
		void partition_optimal() override;

		std::string self_information() override;
	};
}