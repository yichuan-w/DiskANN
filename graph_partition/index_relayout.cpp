//
// Created by Songlin Wu on 2022/6/30.
//
#include <chrono>
#include <string>
#include <memory>
#include <stdexcept>
#include <set>
#include <vector>
#include <iostream>
#include <fstream>
#include <limits>
#include <cstring>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <utility>
#ifdef _OPENMP
#include <omp.h>
#endif
#include <cmath>
#include <mutex>
#include <queue>
#include <random>
#include <sys/stat.h>
#include <errno.h>

// 定义必要的类型和常量
typedef uint64_t _u64;
typedef uint32_t _u32;

#define SECTOR_LEN (_u64) 4096
#define READ_SECTOR_LEN (size_t) 4096
#define READ_SECTOR_OFFSET(node_id) \
  ((_u64) node_id / nnodes_per_sector  + 1) * READ_SECTOR_LEN + ((_u64) node_id % nnodes_per_sector) * max_node_len
#define INF 0xffffffff

#define DEFAULT_MODE 0
#define GRAPH_ONLY 1
#define EMB_ONLY 2

const std::string partition_index_filename = "_tmp.index";

#define GRAPH_LAYOUT_IN_DISK false
#define EMB_LAYOUT_IN_DISK false

// 简化的命名空间
namespace diskann {
  extern std::basic_ostream<char> cout;
  extern std::basic_ostream<char> cerr;
}

// 简化的文件大小获取函数
inline _u64 get_file_size(const std::string& fname) {
  std::ifstream reader(fname, std::ios::binary | std::ios::ate);
  if (!reader.fail() && reader.is_open()) {
    _u64 end_pos = reader.tellg();
    reader.close();
    return end_pos;
  } else {
    std::cerr << "Could not open file: " << fname << std::endl;
    return 0;
  }
}

// 简化的磁盘索引元数据获取函数
std::pair<bool, std::vector<_u64> > get_disk_index_meta(const std::string& path) {
  std::ifstream fin(path, std::ios::binary);
  if (!fin.is_open()) {
    return std::make_pair(false, std::vector<_u64>());
  }

  int meta_n, meta_dim;
  const int expected_new_meta_n = 9;
  const int expected_new_meta_n_with_reorder_data = 12;
  const int old_meta_n = 11;
  bool is_new_version = true;
  std::vector<_u64> metas;

  fin.read((char*)(&meta_n), sizeof(int));
  fin.read((char*)(&meta_dim), sizeof(int));

  if (meta_n == expected_new_meta_n ||
          meta_n == expected_new_meta_n_with_reorder_data) {
      metas.resize(meta_n);
      fin.read((char *)(metas.data()), sizeof(_u64) * meta_n);
  } else {
      is_new_version = false;
      metas.resize(old_meta_n);
      fin.seekg(0, std::ios::beg);
      fin.read((char *)(metas.data()), sizeof(_u64) * old_meta_n);
  }
  fin.close();
  return std::make_pair(is_new_version, metas);
}

// 简化的缓存输出流类
class cached_ofstream {
private:
  std::ofstream file_;
  _u64 buffer_size_;
  std::unique_ptr<char[]> buffer_;
  _u64 buffer_pos_;
  _u64 total_written_;

public:
  cached_ofstream(const std::string& filename, _u64 buffer_size = 64 * 1024 * 1024)
      : buffer_size_(buffer_size), buffer_pos_(0), total_written_(0) {
    file_.open(filename, std::ios::binary);
    if (!file_.is_open()) {
      throw std::runtime_error("Could not open file: " + filename);
    }
    buffer_ = std::unique_ptr<char[]>(new char[buffer_size_]);
  }

  void write(const char* data, _u64 size) {
    if (buffer_pos_ + size <= buffer_size_) {
      // 数据可以放入缓冲区
      memcpy(buffer_.get() + buffer_pos_, data, size);
      buffer_pos_ += size;
    } else {
      // 需要刷新缓冲区
      flush();
      if (size > buffer_size_) {
        // 数据太大，直接写入文件
        file_.write(data, size);
        total_written_ += size;
      } else {
        // 数据可以放入新的缓冲区
        memcpy(buffer_.get(), data, size);
        buffer_pos_ = size;
      }
    }
  }

  void flush() {
    if (buffer_pos_ > 0) {
      file_.write(buffer_.get(), buffer_pos_);
      total_written_ += buffer_pos_;
      buffer_pos_ = 0;
    }
  }

  ~cached_ofstream() {
    flush();
    file_.close();
  }
};

// Write DiskANN sector data according to graph-partition layout 
// The new index data
template <typename T>
void relayout(const char* indexname, const char* partition_name, const int mode) {
  _u64                               C;
  _u64                               _partition_nums;
  _u64                               _nd;
  _u64                               max_node_len;
  std::vector<std::vector<unsigned> > layout;
  std::vector<std::vector<unsigned> > _partition;

  std::ifstream part(partition_name);
  part.read((char*) &C, sizeof(_u64));
  part.read((char*) &_partition_nums, sizeof(_u64));
  part.read((char*) &_nd, sizeof(_u64));
  std::cout << "C: " << C << " partition_nums:" << _partition_nums
            << " _nd:" << _nd << std::endl;
  
  auto meta_pair = get_disk_index_meta(indexname);
  _u64 actual_index_size = get_file_size(indexname);
  _u64 expected_file_size, expected_npts;

  if (meta_pair.first) {
      // new version
      expected_file_size = meta_pair.second.back();
      expected_npts = meta_pair.second.front();
  } else {
      expected_file_size = meta_pair.second.front();
      expected_npts = meta_pair.second[1];
  }

  if (expected_file_size != actual_index_size) {
    std::cout << "File size mismatch for " << indexname
                  << " (size: " << actual_index_size << ")"
                  << " with meta-data size: " << expected_file_size << std::endl;
    exit(-1);
  }
  if (expected_npts != _nd) {
    std::cout << "expect _nd: " << _nd
                  << " actual _nd: " << expected_npts << std::endl;
    exit(-1);
  }

  auto _dim = meta_pair.second[1];
  max_node_len = meta_pair.second[3];
  unsigned nnodes_per_sector = meta_pair.second[4];
  if (mode == DEFAULT_MODE && SECTOR_LEN / max_node_len != C) {
    std::cout << "nnodes per sector: " << SECTOR_LEN / max_node_len << " C: " << C
                  << std::endl;
    exit(-1);
  }

  layout.resize(_partition_nums);
  for (unsigned i = 0; i < _partition_nums; i++) {
    unsigned s;
    part.read((char*) &s, sizeof(unsigned));
    layout[i].resize(s);
    part.read((char*) layout[i].data(), sizeof(unsigned) * s);
  }
  part.close();

  _u64            read_blk_size = 64 * 1024 * 1024;
  _u64            write_blk_size = read_blk_size;

  std::string partition_path(partition_name);
  partition_path = partition_path.substr(0, partition_path.find_last_of('.')) + partition_index_filename;
  cached_ofstream diskann_writer(partition_path, write_blk_size);
  // cached_ifstream diskann_reader(indexname, read_blk_size);

  std::unique_ptr<char[]> sector_buf = std::unique_ptr<char[]>(new char[SECTOR_LEN]);

  // this time, we load all index into mem;
  std::cout << "nnodes per sector "<<nnodes_per_sector << std::endl;
  _u64 file_size = READ_SECTOR_LEN + READ_SECTOR_LEN * ((_nd + nnodes_per_sector - 1) / nnodes_per_sector);
  std::unique_ptr<char[]> mem_index =
      std::unique_ptr<char[]>(new char[file_size]);
  std::ifstream diskann_reader(indexname);
  diskann_reader.read(mem_index.get(),file_size);
  std::cout << "C: " << C << " partition_nums:" << _partition_nums
            << " _nd:" << _nd << std::endl;

  const _u64 disk_file_size = _partition_nums * SECTOR_LEN + SECTOR_LEN;
  if (meta_pair.first) {
    char* meta_buf = mem_index.get() + 2 * sizeof(int);
    *(reinterpret_cast<_u64*>(meta_buf + 4 * sizeof(_u64))) = C;
    *(reinterpret_cast<_u64*>(meta_buf + (meta_pair.second.size()-1) * sizeof(_u64)))
        = disk_file_size;
  } else {
    _u64* meta_buf = reinterpret_cast<_u64*>(mem_index.get());
    *meta_buf = disk_file_size;
    *(meta_buf + 4) = C;
  }
  std::cout << "size " << disk_file_size << std::endl;

  diskann_writer.write((char*) mem_index.get(), SECTOR_LEN);  // copy meta data;

  auto graph_node_len = max_node_len - sizeof(T) * _dim;
  auto emb_node_len = sizeof(T) * _dim;
  std::cout << "max_node_len: " << max_node_len << "graph_node_len: " \
            << graph_node_len << ", emb_node_len: " << emb_node_len << std::endl;
  for (unsigned i = 0; i < _partition_nums; i++) {
    if (i % 100000 == 0) {
      std::cout << "relayout has done " << (float) i / _partition_nums
                    << std::endl;
      std::cout.flush();
    }
    memset(sector_buf.get(), 0, SECTOR_LEN);
    uint64_t start_offset = 0;
    if (mode == GRAPH_ONLY && GRAPH_LAYOUT_IN_DISK || mode == EMB_ONLY && EMB_LAYOUT_IN_DISK) {
      // copy layout data
      unsigned layout_size = layout[i].size();
      memcpy((char*) sector_buf.get(),
             (char*) (&layout_size), sizeof(unsigned));
      memcpy((char*) sector_buf.get() + sizeof(unsigned),
             (char*) layout[i].data(), layout[i].size() * sizeof(unsigned));
      start_offset = sizeof(unsigned) + C * sizeof(unsigned);
      if (layout[i].size() > C) {
        std::cout << "error layout" << std::endl;
        exit(-1);
      }
    }
    for (unsigned j = 0; j < layout[i].size(); j++) {
      unsigned id = layout[i][j];
      if (mode == DEFAULT_MODE) {
        uint64_t index_offset = READ_SECTOR_OFFSET(id);
        uint64_t buf_offset = start_offset + (uint64_t)j * max_node_len;
        memcpy((char*) sector_buf.get() + buf_offset,
              (char*) mem_index.get() + index_offset, max_node_len);
      } else if (mode == GRAPH_ONLY) {
        uint64_t index_offset = READ_SECTOR_OFFSET(id) + emb_node_len;
        uint64_t buf_offset = start_offset + (uint64_t)j * graph_node_len;
        memcpy((char*) sector_buf.get() + buf_offset,
              (char*) mem_index.get() + index_offset, graph_node_len);
      } else if (mode == EMB_ONLY) {
        uint64_t index_offset = READ_SECTOR_OFFSET(id);
        uint64_t buf_offset = start_offset + (uint64_t)j * emb_node_len;
        memcpy((char*) sector_buf.get() + buf_offset,
              (char*) mem_index.get() + index_offset, emb_node_len);
      }
    }
    // memcpy((char*)sector_buf.get() + C*max_node_len, (char*)layout[i].data(), sizeof(unsigned));
    diskann_writer.write(sector_buf.get(), SECTOR_LEN);
  }
  std::cout << "Relayout index." << std::endl;
}

int main(int argc, char** argv) {
  char* indexName = argv[1];
  char* partitonName = argv[2];
  char* data_type = argv[3];
  int mode = std::stoi(argv[4]);

  if (mode == DEFAULT_MODE) {
    std::cout << "relayout all." << std::endl;
  } else if (mode == GRAPH_ONLY) {
    std::cout << "relayout Graph." << std::endl;
  } else if (mode == EMB_ONLY) {
    std::cout << "relayout Emb." << std::endl;
  } else {
    std::cout << "mode not support" << std::endl;
    exit(-1);
  }

  if (std::string(data_type) == std::string("uint8")) {
    relayout<uint8_t>(indexName, partitonName, mode);
  } else if (std::string(data_type) == std::string("float")) {
    relayout<float>(indexName, partitonName, mode);
  } else {
    std::cout << "not support type" << std::endl;
    exit(-1);
  }
  return 0;
}