#include "buffer_pool_manager.h"

/**
 * @brief 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @param frame_id 帧页id指针,返回成功找到的可替换帧id
 * @return true: 可替换帧查找成功 , false: 可替换帧查找失败
 */
bool BufferPoolManager::FindVictimPage(frame_id_t *frame_id) {
    // TODO:
    //  1 使用BufferPoolManager::free_list_判断缓冲池是否已满需要淘汰页面
    //  1.1 未满获得frame
    //  1.2 已满使用lru_replacer中的方法选择淘汰页面
    return true;
}

/**
 * @brief 更新页面数据, 为脏页则需写入磁盘，更新page元数据(data, is_dirty, page_id)和page table
 *
 * @param page 写回页指针
 * @param new_page_id 写回页新page_id
 * @param new_frame_id 写回页新帧frame_id
 */
void BufferPoolManager::UpdatePage(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // TODO:
    //  1 如果是脏页，写回磁盘，并且把dirty置为false
    //  2 更新page table
    //  3 重置page的data，更新page id
}

/**
 * Fetch the requested page from the buffer pool.
 * 如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 * 如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @param page_id id of page to be fetched
 * @return the requested page
 */
Page *BufferPoolManager::FetchPage(PageId page_id) {
    // TODO:
    //  0.     lock latch
    //  1.     Search the page table for the requested page (P).
    //  1.1    If P exists, pin it and return it immediately.
    //  1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //         Note that pages are always found from the free list first.
    //  2.     If R is dirty, write it back to the disk.
    //  3.     Delete R from the page table and insert P.
    //  4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    std::scoped_lock lock{latch_};
    // Page(R) for exchange
    frame_id_t *frame_id = new frame_id_t;

    bool flag = true;  // flag表示现有的都被pin住了
    for (int i = 0; i < int(pool_size_ - free_list_.size()); i++) {
        if (pages_[i].pin_count_ <= 0) {
            flag = false;
            break;
        }
    }
    if (flag && free_list_.empty()) return nullptr;  //如果全部pin住了那缓冲池现在就等于是用不了了

    // if p exists
    if (page_table_.find(page_id) != page_table_.end()) {
        replacer_->Pin(page_table_[page_id]);
        return pages_ + page_table_[page_id];
    } else {
        // if p doesn't exists & should from replacer
        if (free_list_.empty()) {
            replacer_->Victim(frame_id);
        } else {
            // if p doesn't exists & free_list has extra space
            *frame_id = free_list_.front();
            free_list_.pop_front();
        }
    }

    // if page(R) is dirty
    if (pages_[*frame_id].is_dirty_) {
        disk_manager_->write_page(pages_[*frame_id].GetPageId().fd, pages_[*frame_id].GetPageId().page_no,
                                  pages_[*frame_id].GetData(), PAGE_SIZE);
        pages_[*frame_id].is_dirty_ = false;
    }

    page_table_.erase(pages_[*frame_id].id_);
    page_table_.insert({page_id, *frame_id});

    pages_[*frame_id].is_dirty_ = false;
    pages_[*frame_id].id_ = page_id;
    pages_[*frame_id].pin_count_++;
    disk_manager_->read_page(page_id.fd, page_id.page_no, pages_[*frame_id].data_, PAGE_SIZE);

    auto retp = (pages_ + (*frame_id));
    delete frame_id;
    return retp;
}

/**
 * Unpin the target page from the buffer pool. 取消固定pin_count>0的在缓冲池中的page
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page pin count is <= 0 before this call, true otherwise
 */
bool BufferPoolManager::UnpinPage(PageId page_id, bool is_dirty) {
    // TODO:
    //  0. lock latch
    //  1. try to search page_id page P in page_table_
    //  1.1 P在页表中不存在 return false
    //  1.2 P在页表中存在 如何解除一次固定(pin_count)
    //  2. 页面是否需要置脏
    std::scoped_lock lock{latch_};
    frame_id_t *frame_id = new frame_id_t;
    // if p exists
    if (page_table_.find(page_id) != page_table_.end()) {
        *frame_id = page_table_[page_id];
        if (pages_[*frame_id].pin_count_ > 0) {
            pages_[*frame_id].pin_count_--;
        }
        if (pages_[*frame_id].pin_count_ == 0) {
            //!write
            disk_manager_->write_page(pages_[*frame_id].GetPageId().fd, pages_[*frame_id].GetPageId().page_no, pages_[*frame_id].GetData(), PAGE_SIZE);
            replacer_->Unpin(*frame_id);
            if (is_dirty == true) {
                pages_[*frame_id].is_dirty_ = true;
            }
        }
        return true;
    } else
        return false;
}

/**
 * Flushes the target page to disk. 将page写入磁盘；不考虑pin_count
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
bool BufferPoolManager::FlushPage(PageId page_id) {
    // TODO:
    //  0. lock latch
    //  1. 页表查找
    //  2. 存在时如何写回磁盘
    //  3. 写回后页面的脏位
    //  Make sure you call DiskManager::WritePage!
    std::scoped_lock lock{latch_};
    for (size_t i = 0; i < pool_size_; i++) {
        Page *page = &pages_[i];
        if (page->GetPageId().page_no != INVALID_PAGE_ID && page->GetPageId() == page_id) {
            disk_manager_->write_page(page->GetPageId().fd, page->GetPageId().page_no, page->GetData(), PAGE_SIZE);
            page->is_dirty_ = false;
            return true;
        }
    }
    return false;
}

/**
 * Creates a new page in the buffer pool. 相当于从磁盘中移动一个新建的空page到缓冲池某个位置
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
Page *BufferPoolManager::NewPage(PageId *page_id) {
    // TODO:
    //  0.   lock latch
    //  1.   Make sure you call DiskManager::AllocatePage!
    //  2.   If all the pages in the buffer pool are pinned, return nullptr.
    //  3.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
    //  4.   Update P's metadata, zero out memory and add P to the page table. pin_count set to 1.
    //  5.   Set the page ID output parameter. Return a pointer to P.
    std::scoped_lock lock{latch_};
    bool flag = true;  // flag表示现有的都被pin住了
    bool from_free = true;
    for (int i = 0; i < int(pool_size_ - free_list_.size()); i++) {
        if (pages_[i].pin_count_ <= 0) {
            flag = false;
            break;
        }
    }
    if (flag && free_list_.empty()) return nullptr;

    frame_id_t *frame_id = new frame_id_t;
    if (free_list_.empty()) {
        //?不一定对，这里Victim直接pop了
        replacer_->Victim(frame_id);
        from_free = false;
    } else {
        //只有有释放的时候才能push_back。
        *frame_id = free_list_.front();
        free_list_.pop_front();
    }

    page_id->page_no = disk_manager_->AllocatePage(page_id->fd);
    if (!from_free) page_table_.erase(pages_[*frame_id].id_);
    page_table_.insert({*page_id, *frame_id});
    pages_[*frame_id].ResetMemory();
    pages_[*frame_id].id_ = *page_id;
    pages_[*frame_id].pin_count_ = 1;
    replacer_->Pin(*frame_id);
    auto retp = (pages_ + (*frame_id));
    delete frame_id;
    return retp;
}

/**
 * @brief Deletes a page from the buffer pool.
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
bool BufferPoolManager::DeletePage(PageId page_id) {
    // TODO:
    //  0.   lock latch
    //  1.   Make sure you call DiskManager::DeallocatePage!
    //  2.   Search the page table for the requested page (P).
    //  2.1  If P does not exist, return true.
    //  2.2  If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    //  3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free
    //  list.
    std::scoped_lock lock{latch_};
    frame_id_t *frame_id = new frame_id_t;
    // if p exists
    if (page_table_.find(page_id) != page_table_.end()) {
        *frame_id = page_table_[page_id];
        if (pages_[*frame_id].pin_count_ > 0) {
            return false;
        }
        page_table_.erase(page_id);
        pages_[*frame_id].ResetMemory();
        pages_[*frame_id].pin_count_ = 0;
        pages_[*frame_id].is_dirty_ = false;
        free_list_.push_front(*frame_id);
        disk_manager_->DeallocatePage(page_id.page_no);
    }
    delete frame_id;
    return true;
}

/**
 * @brief Flushes all the pages in the buffer pool to disk.
 *
 * @param fd 指定的diskfile open句柄
 */
void BufferPoolManager::FlushAllPages(int fd) {
    // example for disk write
    std::scoped_lock lock{latch_};
    for (size_t i = 0; i < pool_size_; i++) {
        Page *page = &pages_[i];
        if (page->GetPageId().fd == fd && page->GetPageId().page_no != INVALID_PAGE_ID) {
            disk_manager_->write_page(page->GetPageId().fd, page->GetPageId().page_no, page->GetData(), PAGE_SIZE);
            page->is_dirty_ = false;
        }
    }
}