#include "metadata.h"
#include <fstream>
#include <sstream>

namespace ECProject
{
  static const std::unordered_map<std::string, MultiStripePR> mspr_map = {
    {"RAND", RAND},
    {"DISPERSED", DISPERSED},
    {"AGGREGATED", AGGREGATED},
    {"HORIZONTAL", HORIZONTAL},
    {"VERTICAL", VERTICAL}
  };

  static const std::unordered_map<std::string, PlacementRule> pr_map = {
    {"FLAT", FLAT},
    {"RANDOM", RANDOM},
    {"OPTIMAL", OPTIMAL}
  };

  static const std::unordered_map<std::string, ECTYPE> ec_map = {
    {"RS", RS},
    {"AZURE_LRC", AZURE_LRC},
    {"AZURE_LRC_1", AZURE_LRC_1},
    {"OPTIMAL_LRC", OPTIMAL_LRC},
    {"OPTIMAL_CAUCHY_LRC", OPTIMAL_CAUCHY_LRC},
    {"UNIFORM_CAUCHY_LRC", UNIFORM_CAUCHY_LRC},
    {"PC", PC},
    {"Hierachical_PC", Hierachical_PC},
    {"HV_PC", HV_PC}
  };

  ECFAMILY check_ec_family(ECTYPE ec_type)
  {
    if (ec_type == AZURE_LRC || ec_type == AZURE_LRC_1 ||
        ec_type == OPTIMAL_LRC || ec_type == OPTIMAL_CAUCHY_LRC ||
        ec_type == UNIFORM_CAUCHY_LRC) {
      return LRCs;
    } else if (ec_type == PC || ec_type == Hierachical_PC ||
              ec_type == HV_PC) {
      return PCs;
    } else {
      return RSCodes;
    }
  }

  ErasureCode* ec_factory(ECTYPE ec_type, CodingParameters cp)
  {
    ErasureCode *ec;
    if (ec_type == RS) {
      ec = new RSCode(cp.k, cp.m);
    } else if (ec_type == ERS) {
      ec = new EnlargedRSCode(cp.k, cp.m);
      ec->init_coding_parameters(cp);
    } else if (ec_type == AZURE_LRC) {
      ec = new Azu_LRC(cp.k, cp.l, cp.g);
    } else if (ec_type == AZURE_LRC_1) {
      ec = new Azu_LRC_1(cp.k, cp.l, cp.g);
    } else if (ec_type == OPTIMAL_LRC) {
      ec = new Opt_LRC(cp.k, cp.l, cp.g);
    } else if (ec_type == OPTIMAL_CAUCHY_LRC) {
      ec = new Opt_Cau_LRC(cp.k, cp.l, cp.g);
    } else if (ec_type == UNIFORM_CAUCHY_LRC) {
      ec = new Uni_Cau_LRC(cp.k, cp.l, cp.g);
    } else if (ec_type == PC) {
      ec = new ProductCode(cp.k1, cp.m1, cp.k2, cp.m2);
    } else if (ec_type == Hierachical_PC) {
      ec = new HPC(cp.k1, cp.m1, cp.k2, cp.m2);
      ec->init_coding_parameters(cp);
    } else if (ec_type == HV_PC) {
      ec = new HVPC(cp.k1, cp.m1, cp.k2, cp.m2);
    } else {
      ec = nullptr;
    }
    return ec;
  }

  RSCode* rs_factory(ECTYPE ec_type, CodingParameters cp)
  {
    RSCode *rs;
    if (ec_type == RS) {
      rs = new RSCode(cp.k, cp.m);
    } else if (ec_type == ERS) {
      rs = new EnlargedRSCode(cp.k, cp.m);
      rs->init_coding_parameters(cp);
    } else {
      rs = nullptr;
    }
    return rs;
  }

  LocallyRepairableCode* lrc_factory(ECTYPE ec_type, CodingParameters cp)
  {
    LocallyRepairableCode *lrc;
    if (ec_type == AZURE_LRC) {
      lrc = new Azu_LRC(cp.k, cp.l, cp.g);
    } else if (ec_type == AZURE_LRC_1) {
      lrc = new Azu_LRC_1(cp.k, cp.l, cp.g);
    } else if (ec_type == OPTIMAL_LRC) {
      lrc = new Opt_LRC(cp.k, cp.l, cp.g);
    } else if (ec_type == OPTIMAL_CAUCHY_LRC) {
      lrc = new Opt_Cau_LRC(cp.k, cp.l, cp.g);
    } else if (ec_type == UNIFORM_CAUCHY_LRC) {
      lrc = new Uni_Cau_LRC(cp.k, cp.l, cp.g);
    } else {
      lrc = nullptr;
    }
    return lrc;
  }

  ProductCode* pc_factory(ECTYPE ec_type, CodingParameters cp)
  {
    ProductCode *pc;
    if (ec_type == PC) {
      pc = new ProductCode(cp.k1, cp.m1, cp.k2, cp.m2);
    } else if (ec_type == Hierachical_PC) {
      pc = new HPC(cp.k1, cp.m1, cp.k2, cp.m2);
      pc->init_coding_parameters(cp);
    } else if (ec_type == HV_PC) {
      pc = new HVPC(cp.k1, cp.m1, cp.k2, cp.m2);
    } else {
      pc = nullptr;
    }
    return pc;
  }

  ErasureCode* clone_ec(ECTYPE ec_type, ErasureCode* ec)
  {
    CodingParameters cp;
    ec->get_coding_parameters(cp);
    return ec_factory(ec_type, cp);
  }

  void parse_args(ParametersInfo& paras, std::string config_file)
  {
    std::unordered_map<std::string, std::string> config;
    std::ifstream file(config_file);
    if (file.is_open()) {
      std::string line;
      while (std::getline(file, line)) {
        if (line[0] == '[' ||line[0] == '#' || line.empty())
          continue;

        std::stringstream ss(line);
        std::string key, value;
        if (std::getline(ss, key, '=') && std::getline(ss, value)) {
          // remove space around '='
          key.erase(key.begin(), std::find_if(key.begin(), key.end(),
                    [](unsigned char ch) { return !std::isspace(ch); }));
          value.erase(value.begin(), std::find_if(value.begin(), value.end(),
                    [](unsigned char ch) { return !std::isspace(ch); }));
          config[key] = value;
        }
      }
      file.close();
    } else {
      std::cerr << "Unable to open " << config_file << std::endl;
      return;
    }

    paras.partial_decoding = (config["partial_decoding"] == "true");
    std::string temp = config["ec_type"];
    paras.ec_type = ec_map.at(temp);
    temp = config["placement_rule"];
    paras.placement_rule = pr_map.at(temp);
    temp = config["multistripe_placement_rule"];
    paras.multistripe_placement_rule = mspr_map.at(temp);
    paras.object_size_upper = std::stoul(config["object_size_upper"]) * 1024;
    paras.x = std::stoi(config["x"]);
    paras.cp.k = std::stoi(config["k"]);
    paras.cp.m = std::stoi(config["m"]);
    paras.cp.l = std::stoi(config["l"]);
    paras.cp.g = std::stoi(config["g"]);
    paras.cp.k1 = std::stoi(config["k1"]);
    paras.cp.m1 = std::stoi(config["m1"]);
    paras.cp.k2 = std::stoi(config["k2"]);
    paras.cp.m2 = std::stoi(config["m2"]);

    if (check_ec_family(paras.ec_type) == LRCs) {
      paras.cp.m = paras.cp.l + paras.cp.g;
    } else if (check_ec_family(paras.ec_type) == PCs) {
      if (paras.ec_type == HV_PC) {
        paras.cp.m = paras.cp.k1 * paras.cp.m2 + paras.cp.k2 * paras.cp.m1;
      } else {
        paras.cp.m = paras.cp.k1 * paras.cp.m2 + paras.cp.k2 * paras.cp.m1
            + paras.cp.m1 * paras.cp.m2;
      }
    }
  }
}
