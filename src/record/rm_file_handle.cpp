#include "rm_file_handle.h"
#define DEBUG 0
int ONCE = 0;
/**
 * @brief 由Rid得到指向RmRecord的指针
 *
 * @param rid 指定记录所在的位置
 * @return std::unique_ptr<RmRecord>
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid &rid, Context *context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    if(RmFileHandle::is_record(rid)) {
        RmPageHandle page_handle = fetch_page_handle(rid.page_no);
        return std::unique_ptr<RmRecord>(new RmRecord({file_hdr_.record_size, page_handle.get_slot(rid.slot_no)}));
    }
    return std::unique_ptr<RmRecord>(new RmRecord({file_hdr_.record_size, nullptr}));
}

/**
 * @brief 在该记录文件（RmFileHandle）中插入一条记录
 *
 * @param buf 要插入的数据的地址
 * @return Rid 插入记录的位置
 */
Rid RmFileHandle::insert_record(char *buf, Context *context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    if(!(--ONCE)) std::cout<<"max_record_size:"<<file_hdr_.num_records_per_page<<std::endl;
    RmPageHandle page_handle = create_page_handle();
    int new_slot_no = Bitmap::first_bit(0, page_handle.bitmap, file_hdr_.num_records_per_page);
    Bitmap::set(page_handle.bitmap, new_slot_no);
    memcpy(page_handle.get_slot(new_slot_no), buf, file_hdr_.record_size);
    page_handle.page_hdr->num_records++;
    if(DEBUG) std::cout<<"insert_record: "<<page_handle.page_hdr->num_records<<" in "<<page_handle.page->GetPageId().page_no<<std::endl;
    if(page_handle.page_hdr->num_records == file_hdr_.num_records_per_page)
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
    buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
    if(DEBUG) std::cout<<"here: "<<new_slot_no<<" in "<<page_handle.page->GetPageId().page_no<<std::endl;
    return Rid{page_handle.page->GetPageId().page_no, new_slot_no};
}

/**
 * @brief 在该记录文件（RmFileHandle）中删除一条指定位置的记录
 *
 * @param rid 要删除的记录所在的指定位置
 */
void RmFileHandle::delete_record(const Rid &rid, Context *context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;
    if(DEBUG) std::cout<<"del: "<<page_handle.page->GetPageId().page_no<<std::endl;
    if(page_handle.page_hdr->num_records + 1 == file_hdr_.num_records_per_page)
        release_page_handle(page_handle);
    buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
}

/**
 * @brief 更新指定位置的记录
 *
 * @param rid 指定位置的记录
 * @param buf 新记录的数据的地址
 */
void RmFileHandle::update_record(const Rid &rid, char *buf, Context *context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);
    if(DEBUG) std::cout<<"update:"<<rid.slot_no<<" in "<<rid.page_no<<std::endl;
    buffer_pool_manager_->UnpinPage(page_handle.page->GetPageId(), true);
}

/** -- 以下为辅助函数 -- */
/**
 * @brief 获取指定页面编号的page handle
 *
 * @param page_no 要获取的页面编号
 * @return RmPageHandle 返回给上层的page_handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception
    if(page_no == INVALID_PAGE_ID) throw(PageNotExistError("hhh",page_no));
    Page* page = buffer_pool_manager_->FetchPage(PageId{fd_, page_no});
    //if(DEBUG) std::cout<<"fetch: "<<page->GetPageId().page_no<<std::endl;
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @brief 创建一个新的page handle
 *
 * @return RmPageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_
    Page* new_page = buffer_pool_manager_->NewPage(new PageId({fd_}));
    RmPageHandle new_page_handle = RmPageHandle(&file_hdr_, new_page);
    new_page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = new_page->GetPageId().page_no;
    if(DEBUG) std::cout<<"create: "<<file_hdr_.first_free_page_no<<"->"<<new_page_handle.page_hdr->next_free_page_no<<std::endl;
    new_page_handle.page_hdr->num_records = 0;
    file_hdr_.num_pages++;
    Bitmap::init(new_page_handle.bitmap, file_hdr_.bitmap_size);
    return new_page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层
    if(file_hdr_.first_free_page_no == -1)
        return create_new_page_handle();
    return fetch_page_handle(file_hdr_.first_free_page_no);
}

/**
 * @brief 当page handle中的page从已满变成未满的时候调用
 *
 * @param page_handle
 * @note only used in delete_record()
 */
void RmFileHandle::release_page_handle(RmPageHandle &page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->GetPageId().page_no;
    if(DEBUG) std::cout<<"release: "<<file_hdr_.first_free_page_no<<std::endl;
}

// used for recovery (lab4)
void RmFileHandle::insert_record(const Rid &rid, char *buf) {
    if (rid.page_no < file_hdr_.num_pages) {
        create_new_page_handle();
    }
    RmPageHandle pageHandle = fetch_page_handle(rid.page_no);
    Bitmap::set(pageHandle.bitmap, rid.slot_no);
    pageHandle.page_hdr->num_records++;
    if (pageHandle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = pageHandle.page_hdr->next_free_page_no;
    }

    char *slot = pageHandle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);

    buffer_pool_manager_->UnpinPage(pageHandle.page->GetPageId(), true);
}