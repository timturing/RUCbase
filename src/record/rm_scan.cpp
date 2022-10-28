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
    if (file_handle_->file_hdr_.first_free_page_no != -1)  // TODO应该不用我初始化吧
    {
        RmPageHandle pagehandle = file_handle_->fetch_page_handle(file_handle_->file_hdr_.first_free_page_no);
        int i = Bitmap::first_bit(1, pagehandle.bitmap, file_handle_->file_hdr_.bitmap_size);
        this->rid_ = Rid{file_handle_->file_hdr_.first_free_page_no, i};
    }else
    {
        rid_=Rid{-1,-1};
    }
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    // TODO 要自己写is_end判断吗
    RmPageHandle pagehandle = file_handle_->fetch_page_handle(rid_.page_no);
    while (1) {
        int nextrid = Bitmap::next_bit(1, pagehandle.bitmap, file_handle_->file_hdr_.bitmap_size, rid_.slot_no);
        if (nextrid == file_handle_->file_hdr_.bitmap_size) {
            // 到达end，要传下一个页面
            rid_.page_no = pagehandle.page_hdr->next_free_page_no;
            rid_.slot_no = -1;
            if (rid_.page_no != -1) {
                pagehandle = file_handle_->fetch_page_handle(rid_.page_no);
            }
            else
            {
                rid_.page_no = -1;
                rid_.slot_no = -1;
                return;
            }
        } else {
            //在本页面中
            rid_.slot_no = nextrid;
            return;
        }
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值

    if(rid_.page_no==-1&&rid_.slot_no==-1)return true;
    return false;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    // Todo: 修改返回值
    return rid_;
}