#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 *
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    file_handle_ = file_handle;
    RmFileHdr _file_hdr = file_handle_->file_hdr_;
    int max_n = _file_hdr.num_records_per_page;
    if(_file_hdr.num_pages == 1) {
        rid_.slot_no = max_n;
        rid_.page_no = 0;
    }
    else {
        RmPageHandle page_handle = file_handle_->fetch_page_handle(1);
        rid_.slot_no = Bitmap::first_bit(1, page_handle.bitmap, max_n);
        rid_.page_no = 1;
    }
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    int page_no = rid_.page_no;
    RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);
    RmFileHdr _file_hdr = file_handle_->file_hdr_;
    int max_n = _file_hdr.num_records_per_page;
    int next_slot_no = Bitmap::next_bit(1, page_handle.bitmap, max_n, rid_.slot_no);
    int next_page_no = page_no;
    if(next_slot_no == max_n) {
        for(int i = page_no + 1; i < _file_hdr.num_pages; ++i) {
            RmPageHandle page_handle = file_handle_->fetch_page_handle(i);
            next_slot_no = Bitmap::first_bit(1, page_handle.bitmap, max_n);
            if(next_slot_no != max_n) {
                next_page_no = i;
                break;
            } 
        }
    }
    rid_.slot_no = next_slot_no;
    rid_.page_no = next_page_no;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    RmFileHdr _file_hdr = file_handle_->file_hdr_;
    return rid_.slot_no == _file_hdr.num_records_per_page;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    // Todo: 修改返回值
    return rid_;
}