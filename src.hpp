#include "fstream.h"
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

// 磁盘事件类型：正常、故障、更换
enum class EventType {
  NORMAL,  // 正常：所有磁盘工作正常
  FAILED,  // 故障：指定磁盘发生故障（文件被删除）
  REPLACED // 更换：指定磁盘被更换（文件被清空）
};

class RAID5Controller {
private:
  std::vector<sjtu::fstream *> drives_; // 磁盘文件对应的 fstream 对象
  int blocks_per_drive_;                // 每个磁盘的块数
  int block_size_;                      // 每个块的大小
  int num_disks_;                       // 磁盘数
  int failed_drive_;                    // 当前处于故障状态的磁盘编号，-1 表示无

  // 计算条带编号与条带内位置
  inline void map_block(int block_id, int &stripe, int &pos_in_stripe) const {
    stripe = block_id / (num_disks_ - 1);
    pos_in_stripe = block_id % (num_disks_ - 1);
  }

  // 给定条带与条带内位置，计算物理磁盘编号与该条带的奇偶校验盘编号
  inline void stripe_layout(int stripe, int pos_in_stripe, int &data_disk, int &parity_disk) const {
    parity_disk = stripe % num_disks_;
    // 数据磁盘从 0..num_disks_-1，跳过 parity_disk
    data_disk = (pos_in_stripe >= parity_disk) ? (pos_in_stripe + 1) : pos_in_stripe;
  }

  // 从指定磁盘读取一个条带块到缓冲区（大小为 block_size_）
  void read_from_disk(int disk, int stripe, char *buf) {
    sjtu::fstream *f = drives_[disk];
    f->clear();
    f->seekg(static_cast<std::streamoff>(stripe) * block_size_, std::ios::beg);
    f->read(buf, block_size_);
    std::streamsize got = f->gcount();
    for (int i = static_cast<int>(got); i < block_size_; ++i) buf[i] = 0;
    // debug
    // fprintf(stderr, "READ d=%d s=%d got=%ld first=%d\n", disk, stripe, (long)got, (int)(unsigned char)buf[0]);
  }

  // 将缓冲区写入指定磁盘的一个条带块
  void write_to_disk(int disk, int stripe, const char *buf) {
    sjtu::fstream *f = drives_[disk];
    f->clear();
    f->seekp(static_cast<std::streamoff>(stripe) * block_size_, std::ios::beg);
    f->write(buf, block_size_);
    f->flush();
    // debug
    // fprintf(stderr, "WRITE d=%d s=%d first=%d\n", disk, stripe, (int)(unsigned char)buf[0]);
  }

  // 计算该条带的异或（奇偶校验），可选择跳过一个磁盘（用于重建）
  void xor_across_stripe(int stripe, int skip_disk, char *out) {
    std::fill(out, out + block_size_, 0);
    std::vector<char> tmp(block_size_);
    for (int d = 0; d < num_disks_; ++d) {
      if (d == skip_disk) continue;
      if (failed_drive_ == d) continue; // 无法读取故障盘
      read_from_disk(d, stripe, tmp.data());
      for (int i = 0; i < block_size_; ++i) out[i] ^= tmp[i];
    }
  }

public:
  RAID5Controller(std::vector<sjtu::fstream *> drives, int blocks_per_drive,
                  int block_size = 4096)
      : drives_(std::move(drives)), blocks_per_drive_(blocks_per_drive),
        block_size_(block_size), num_disks_(static_cast<int>(drives_.size())),
        failed_drive_(-1) {
    // 输入“磁盘”（文件）对应的 fstream* 对象。
    // drives.size() 即为磁盘个数
    // 文件已经存在且大小为 block_size * blocks_per_drive
    // 文件初始数据为全 0
  }

  /**
   * @brief 启动 RAID5 系统
   * @param event_type_ 磁盘事件类型
   * @param drive_id 发生事件的磁盘编号（如果是 NORMAL 则忽略）
   *
   * 如果是 FAILED，对应的磁盘文件会被删除。此时不可再对该文件进行读写。
   * 如果是 REPLACED，对应的磁盘文件会被清空（但文件依然存在）
   * 如果是 NORMAL，所有磁盘正常工作
   * 注：磁盘被替换之前不一定损坏。
   */
  void Start(EventType event_type_, int drive_id) {
    if (event_type_ == EventType::NORMAL) {
      failed_drive_ = -1;
      return;
    }

    if (event_type_ == EventType::FAILED) {
      // 标记为故障，之后读写时进行退化处理
      failed_drive_ = drive_id;
      return;
    }

    // REPLACED：对被更换的磁盘进行重建
    if (event_type_ == EventType::REPLACED) {
      // 被替换磁盘视为可写可读（文件已清空），我们需要用其余磁盘数据重建
      int replaced = drive_id;
      // 重建每个条带
      std::vector<char> buf(block_size_);
      for (int stripe = 0; stripe < blocks_per_drive_; ++stripe) {
        // 计算该条带中 replaced 磁盘应存放的数据（可能是奇偶校验块或数据块）
        int parity_disk = stripe % num_disks_;
        if (replaced == parity_disk) {
          // 重建奇偶校验：异或所有数据块
          std::fill(buf.begin(), buf.end(), 0);
          std::vector<char> tmp(block_size_);
          for (int d = 0; d < num_disks_; ++d) {
            if (d == parity_disk) continue;
            if (failed_drive_ == d) continue;
            read_from_disk(d, stripe, tmp.data());
            for (int i = 0; i < block_size_; ++i) buf[i] ^= tmp[i];
          }
        } else {
          // 重建数据块：P XOR 其他数据块
          // 先异或除 replaced 外的所有盘得到 P XOR 其他数据
          xor_across_stripe(stripe, replaced, buf.data());
        }
        // 写回到被替换的磁盘
        write_to_disk(replaced, stripe, buf.data());
      }
      // 重建完成，系统恢复正常
      failed_drive_ = -1;
      return;
    }
  }

  void Shutdown() {
    // 关闭所有打开的文件，以防未定义行为发生。
    for (auto *f : drives_) {
      if (f && f->is_open()) f->close();
    }
  }

  void ReadBlock(int block_id, char *result) {
    // 读取第 block_id 个块的内容，写入 result 中
    int stripe, pos;
    map_block(block_id, stripe, pos);
    int data_disk, parity_disk;
    stripe_layout(stripe, pos, data_disk, parity_disk);

    if (failed_drive_ == -1 || failed_drive_ == parity_disk) {
      // 直接从数据盘读取
      read_from_disk(data_disk, stripe, result);
    } else if (failed_drive_ == data_disk) {
      // 目标数据盘故障：通过异或重建
      // 计算 P XOR 其他数据（不包含故障盘）
      xor_across_stripe(stripe, failed_drive_, result);
    } else {
      // 其他盘故障但不影响本次读取
      read_from_disk(data_disk, stripe, result);
    }
  }

  void WriteBlock(int block_id, const char *data) {
    // 将 data 中的内容写入第 block_id 个块中
    int stripe, pos;
    map_block(block_id, stripe, pos);
    int data_disk, parity_disk;
    stripe_layout(stripe, pos, data_disk, parity_disk);

    if (failed_drive_ == -1) {
      // 正常模式：读出旧数据与旧奇偶校验，计算新奇偶校验并写回
      std::vector<char> old_data(block_size_), old_parity(block_size_), new_parity(block_size_);
      read_from_disk(data_disk, stripe, old_data.data());
      read_from_disk(parity_disk, stripe, old_parity.data());
      // P_new = P_old XOR D_old XOR D_new
      for (int i = 0; i < block_size_; ++i) new_parity[i] = old_parity[i] ^ old_data[i] ^ data[i];
      // 写数据与新奇偶校验
      write_to_disk(data_disk, stripe, data);
      write_to_disk(parity_disk, stripe, new_parity.data());
    } else if (failed_drive_ == data_disk) {
      // 退化模式：目标数据盘故障，无法写数据，只能更新奇偶校验
      // 方案：重新计算该条带的奇偶校验，使其等于 XOR(所有数据，其中该数据为新值)
      std::vector<char> parity(block_size_);
      std::fill(parity.begin(), parity.end(), 0);
      std::vector<char> tmp(block_size_);
      for (int d = 0; d < num_disks_; ++d) {
        if (d == parity_disk) continue;
        if (d == data_disk) {
          // 使用新数据
          for (int i = 0; i < block_size_; ++i) parity[i] ^= data[i];
        } else if (failed_drive_ != d) {
          read_from_disk(d, stripe, tmp.data());
          for (int i = 0; i < block_size_; ++i) parity[i] ^= tmp[i];
        }
      }
      // 写入新的奇偶校验（若奇偶盘未故障）
      if (failed_drive_ != parity_disk) write_to_disk(parity_disk, stripe, parity.data());
      // 数据写入会在更换磁盘后进行重建
    } else if (failed_drive_ == parity_disk) {
      // 奇偶校验盘故障：直接写数据，奇偶校验在更换后重建
      write_to_disk(data_disk, stripe, data);
    } else {
      // 其他盘故障：不影响该条带的目标与奇偶盘，正常更新
      std::vector<char> old_data(block_size_), old_parity(block_size_), new_parity(block_size_);
      read_from_disk(data_disk, stripe, old_data.data());
      read_from_disk(parity_disk, stripe, old_parity.data());
      for (int i = 0; i < block_size_; ++i) new_parity[i] = old_parity[i] ^ old_data[i] ^ data[i];
      write_to_disk(data_disk, stripe, data);
      write_to_disk(parity_disk, stripe, new_parity.data());
    }
  }

  int Capacity() {
    // 返回磁盘阵列能写入的块的数量（你无需改动此函数）
    return (num_disks_ - 1) * blocks_per_drive_;
  }
};
