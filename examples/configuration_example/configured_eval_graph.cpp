#include <string>

#include <spdlog/spdlog.h>
#include <yaml-cpp/yaml.h>

int main(int argc, char** argv) {
  if (argc < 2) {
    spdlog::error("Configuration file is not provided");
    return 1;
  }

  spdlog::set_level(spdlog::level::debug);

  YAML::Node config = YAML::LoadFile(argv[1]);
  for (auto iter = config.begin(); iter != config.end(); ++iter) {
    auto map_iter = iter->begin();
    spdlog::debug("{}: {}",
                  map_iter->first.as<std::string>(),
                  map_iter->second.IsMap());

    if (map_iter->second.IsMap()) {
      for (auto seq_iter = map_iter->second.begin(); seq_iter != map_iter->second.end(); ++seq_iter) {
        if (seq_iter->first.IsScalar()) {
          spdlog::debug(seq_iter->first.as<std::string>());
        }

        if (seq_iter->second.IsScalar()) {
          spdlog::debug(seq_iter->second.as<std::string>());
        }
      }
    }
  }

  return 0;
}