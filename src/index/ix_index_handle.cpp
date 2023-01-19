#include "ix_index_handle.h"

#include "ix_scan.h"

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    // disk_manager管理的fd对应的文件中，设置从原来编号+1开始分配page_no
    disk_manager_->set_fd2pageno(fd, disk_manager_->get_fd2pageno(fd) + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 *
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return 返回目标叶子结点
 * @note need to Unpin the leaf node outside!
 */
IxNodeHandle *IxIndexHandle::FindLeafPage(const char *key, Operation operation, Transaction *transaction) {
    // Todo:
    // 1. 获取根节点
    // 2. 从根节点开始不断向下查找目标key
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点
    // 蟹行协议
    if (operation == Operation::FIND) {
        page_id_t rootpageno = file_hdr_.root_page;
        page_id_t pageno;
        IxNodeHandle *now = FetchNode(rootpageno);
        now->mutex_.lock_shared();
        // 只要还不是叶子节点,就在内部节点一路往下找
        while (!now->page_hdr->is_leaf) {
            pageno = now->InternalLookup(key);
            auto old = now;
            now = FetchNode(pageno);
            now->mutex_.lock_shared();
            old->mutex_.unlock_shared();
        }
        return now;
    }
    else if(operation == Operation::INSERT)
    {
        page_id_t rootpageno = file_hdr_.root_page;
        page_id_t pageno;
        IxNodeHandle *now = FetchNode(rootpageno);
        now->mutex_.lock();
        transaction->AddIntoPageSet(now->page);
        // 只要还不是叶子节点,就在内部节点一路往下找
        while (!now->page_hdr->is_leaf) {
            pageno = now->InternalLookup(key);
            now = FetchNode(pageno);
            now->mutex_.lock();
            if(now->GetSize()+1<now->GetMaxSize())//安全的节点
            {
                while(!transaction->GetPageSet()->empty())
                {
                    auto tmp_page = transaction->GetPageSet()->front();
                    IxNodeHandle *tmp = FetchNode(tmp_page->GetPageId().page_no);
                    tmp->mutex_.unlock();
                    transaction->GetPageSet()->pop_front();
                }
            }
            transaction->AddIntoPageSet(now->page);
        }
        return now;
    }
    else if(operation == Operation::DELETE)
    {
        page_id_t rootpageno = file_hdr_.root_page;
        page_id_t pageno;
        IxNodeHandle *now = FetchNode(rootpageno);
        // 只要还不是叶子节点,就在内部节点一路往下找
        while (!now->page_hdr->is_leaf) {
            pageno = now->InternalLookup(key);
            now = FetchNode(pageno);
        }
        return now;
    }
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::GetValue(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中
    // TODO 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁
    IxNodeHandle *leaf = FindLeafPage(key, Operation::FIND, transaction);
    Rid **value = new (Rid *);
    bool flag = leaf->LeafLookup(key, value);
    if (flag) result->push_back(**value);
    // delete value;
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);  // 这里也是unpin吗
    leaf->mutex_.unlock_shared();
    return flag;
}

/**
 * @brief 将指定键值对插入到B+树中
 *
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return 是否插入成功
 */
bool IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // TODO：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁
    // std::scoped_lock lock{root_latch_};
    IxNodeHandle *leaf = FindLeafPage(key, Operation::INSERT, transaction);
    // leaf->mutex_.unlock_shared();
    // leaf->mutex_.lock();
    int before_insert = leaf->page_hdr->num_key;
    int after_insert = leaf->Insert(key, value);
    if (leaf->page_hdr->next_leaf == 1) file_hdr_.last_leaf = leaf->GetPageNo();
    if (after_insert > before_insert) {
        // 如果插入成功,但是叶子节点满了,就要分裂
        if (leaf->page_hdr->num_key == leaf->GetMaxSize()) {
            // 分裂
            IxNodeHandle *newleaf = Split(leaf);
            // newleaf->mutex_.lock();
            // 更新父节点
            InsertIntoParent(leaf, newleaf->keys, newleaf, transaction);
            // newleaf->mutex_.unlock();
            if (newleaf->page_hdr->next_leaf == 1) {
                file_hdr_.last_leaf = newleaf->GetPageNo();
                // disk_manager_->write_page(fd_, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
            }
            buffer_pool_manager_->UnpinPage(newleaf->GetPageId(), true);
        }
        buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
        // leaf->mutex_.unlock();
        return true;
    }
    // leaf->mutex_.unlock();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    while(!transaction->GetPageSet()->empty())
    {
        auto tmp_page = transaction->GetPageSet()->front();
        IxNodeHandle *tmp = FetchNode(tmp_page->GetPageId().page_no);
        tmp->mutex_.unlock();
        transaction->GetPageSet()->pop_front();
    }
    return false;
}

/**
 * @brief 将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 *
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note 本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::Split(IxNodeHandle *node) {
    // Todo:
    // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
    //    需要初始化新节点的page_hdr内容
    // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
    //    为新节点分配键值对，更新旧节点的键值对数记录
    // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())
    int mid = node->page_hdr->num_key / 2;
    IxNodeHandle *new_node = CreateNode();
    // TODO next_free_pageno?
    new_node->page_hdr->is_leaf = node->page_hdr->is_leaf;
    new_node->page_hdr->num_key = node->page_hdr->num_key - mid;
    new_node->page_hdr->parent = node->page_hdr->parent;
    new_node->page_hdr->next_free_page_no = node->page_hdr->next_free_page_no;
    node->page_hdr->num_key = mid;
    for (int i = 0; i < new_node->page_hdr->num_key; i++) {
        // memcpy(new_node->keys+i, node->keys + mid + i, sizeof(ColType));
        // memcpy(new_node->rids+i, node->rids + mid + i, sizeof(Rid));
        new_node->set_key(i, node->get_key(mid + i));
        new_node->set_rid(i, *node->get_rid(mid + i));
    }
    if (node->page_hdr->is_leaf) {
        // 如果是叶子节点
        new_node->page_hdr->next_leaf = node->page_hdr->next_leaf;
        new_node->page_hdr->prev_leaf = node->GetPageNo();
        node->page_hdr->next_leaf = new_node->GetPageNo();
        if (new_node->page_hdr->next_leaf != INVALID_PAGE_ID) {
            IxNodeHandle *next = FetchNode(new_node->page_hdr->next_leaf);
            next->page_hdr->prev_leaf = new_node->GetPageNo();
            buffer_pool_manager_->UnpinPage(next->GetPageId(), true);  //?
        }
    } else {
        // 如果不是
        new_node->page_hdr->next_leaf = INVALID_PAGE_ID;
        new_node->page_hdr->prev_leaf = INVALID_PAGE_ID;
        for (int i = 0; i < new_node->page_hdr->num_key; i++) {
            maintain_child(new_node, i);
        }
    }

    return new_node;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::InsertIntoParent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                     Transaction *transaction) {
    // Todo:
    // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
    // 2. 获取原结点（old_node）的父亲结点
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 提示：记得unpin page

    // 如果old_node是根节点,则需要新建一个根节点
    if (old_node->page_hdr->parent == INVALID_PAGE_ID) {
        IxNodeHandle *new_root = CreateNode();
        new_root->page_hdr->is_leaf = false;
        new_root->page_hdr->num_key = 0;
        new_root->page_hdr->parent = INVALID_PAGE_ID;
        new_root->page_hdr->next_leaf = INVALID_PAGE_ID;
        new_root->page_hdr->prev_leaf = INVALID_PAGE_ID;
        // new_root->Insert(old_node->get_key(0), *old_node->get_rid(0)); //
        // FIXED：因为不是传子节点的子树，而是把子节点当子树 new_root->Insert(new_node->get_key(0),
        // *new_node->get_rid(0));
        new_root->set_key(0, old_node->get_key(0));
        new_root->set_rid(0, Rid{old_node->GetPageNo(), -1});
        new_root->set_key(1, new_node->get_key(0));
        new_root->set_rid(1, Rid{new_node->GetPageNo(), -1});
        new_root->page_hdr->num_key = 2;
        // 将old_node和new_node的父节点设置为new_root
        maintain_child(new_root, 0);
        maintain_child(new_root, 1);
        file_hdr_.root_page = new_root->GetPageNo();
        // disk_manager_->write_page(fd_, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
        buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    } else {
        // 如果old_node不是根节点,则直接在其父节点中插入key
        IxNodeHandle *parent = FetchNode(old_node->page_hdr->parent);
        // parent->mutex_.lock();
        parent->Insert(new_node->get_key(0), Rid{new_node->GetPageNo(), -1});
        // if (old_node->page_hdr->next_free_page_no == INVALID_PAGE_ID) {
        //     file_hdr_.last_leaf = new_node->GetPageNo();
        //     disk_manager_->write_page(fd_, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
        // }
        // 如果父节点满了,就要分裂
        if (parent->page_hdr->num_key == new_node->GetMaxSize()) {
            IxNodeHandle *newparent = Split(parent);
            // newparent->mutex_.lock();
            // 更新根节点
            InsertIntoParent(parent, newparent->keys, newparent, transaction);
            // newparent->mutex_.unlock();
        }
        // parent->mutex_.unlock();
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    }
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 *
 * @param key 要删除的key值
 * @param transaction 事务指针
 * @return 是否删除成功
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3.
    // 如果删除成功且删除后该结点小于半满，需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁

    // TODO 注意要处理并发
    std::scoped_lock lock{root_latch_};
    // 获取叶子结点
    IxNodeHandle *leaf = FindLeafPage(key, Operation::DELETE, transaction);
    // leaf->mutex_.unlock_shared();
    // 删除键值对
    int before_delete = leaf->page_hdr->num_key;
    int after_delete = leaf->Remove(key);
    if (before_delete > after_delete) {
        // 如果删除成功,则需要调用CoalesceOrRedistribute来进行合并或重分配操作
        bool ifdelete = false;
        if (leaf->page_hdr->num_key < leaf->GetMinSize()) {
            ifdelete = CoalesceOrRedistribute(leaf, transaction);
        } else {
            //?这个应该写在这吗
            maintain_parent(leaf);
        }
        if (ifdelete) {
            // 如果需要删除叶子结点,则需要在事务的delete_page_set中添加删除结点的对应页面
            transaction->AddIntoDeletedPageSet(leaf->page);
        }
    }
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return before_delete > after_delete;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::CoalesceOrRedistribute(IxNodeHandle *node, Transaction *transaction) {
    // Todo:
    // 1. 判断node结点是否为根节点
    //    1.1 如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
    //    1.2 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false，否则执行2
    // 2. 获取node结点的父亲结点
    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    // 4. 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
    // NodeMinSize*2)，则只需要重新分配键值对（调用Redistribute函数）
    // 5. 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）
    // TODO root_is_latched将会在并发操作中使用
    if (node->IsRootPage()) {
        // 如果是根节点,则需要调用AdjustRoot()函数来进行处理
        return AdjustRoot(node);
    } else {
        // 获取node结点的父亲结点
        IxNodeHandle *parent = FetchNode(node->GetParentPageNo());
        // 寻找node结点的兄弟结点
        int index = parent->find_child(node);
        IxNodeHandle *neighbor;
        if (index > 0) {
            // 优先选取前驱结点
            neighbor = FetchNode(parent->get_rid(index - 1)->page_no);
        } else {
            // index==0时，选取后继结点
            neighbor = FetchNode(parent->get_rid(index + 1)->page_no);
        }
        // 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点
        if (node->page_hdr->num_key + neighbor->page_hdr->num_key >= node->GetMinSize() * 2) {
            // 则只需要重新分配键值对
            Redistribute(neighbor, node, parent, index);
        } else {
            // 否则需要合并两个结点
            return Coalesce(&neighbor, &node, &parent, index, transaction);  // TODO是要return吗
        }
        //? 这里是写maintain吗
        maintain_parent(node);
        maintain_parent(neighbor);
        // buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
    }
    return false;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 *
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesceOrRedistribute()
 */
bool IxIndexHandle::AdjustRoot(IxNodeHandle *old_root_node) {
    // Todo:
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    // 3. 除了上述两种情况，不需要进行操作

    // 如果old_root_node是内部结点，并且大小为1
    if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
        // 则直接把它的孩子更新成新的根结点
        IxNodeHandle *child = FetchNode(old_root_node->get_rid(0)->page_no);
        child->page_hdr->parent = INVALID_PAGE_ID;
        //? 需要unpin和delete吗
        file_hdr_.root_page = child->GetPageNo();
        release_node_handle(*child);
        return true;
    }
    // 如果old_root_node是叶结点，且大小为0
    else if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
        // 则直接更新root page
        //? 需要delete吗
        file_hdr_.root_page = INVALID_PAGE_ID;
        release_node_handle(*FetchNode(old_root_node->GetPageNo()));
        return true;
    }
    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::Redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    // Todo:
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    // 2. 从neighbor_node中移动一个键值对到node结点中
    // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
    // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论

    // 通过index判断neighbor_node是否为node的前驱结点
    if (index == 0) {
        // 从neighbor_node中移动一个键值对到node结点中
        node->insert_pair(node->GetSize(), neighbor_node->get_key(0), *neighbor_node->get_rid(0));
        neighbor_node->erase_pair(0);
        // 更新父节点中的相关信息
        parent->set_key(0, neighbor_node->get_key(0));
        // 修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
        maintain_child(node, node->GetSize() - 1);
    } else {
        // 从neighbor_node中移动一个键值对到node结点中
        node->insert_pair(0, neighbor_node->get_key(neighbor_node->GetSize() - 1),
                          *neighbor_node->get_rid(neighbor_node->GetSize() - 1));
        neighbor_node->erase_pair(neighbor_node->GetSize() - 1);
        // 更新父节点中的相关信息
        parent->set_key(index, node->get_key(0));
        // 修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
        maintain_child(node, 0);
    }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::Coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction) {
    // Todo:
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf
    //! 还有问题
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    if (index == 0) {
        IxNodeHandle *temp = *neighbor_node;
        *neighbor_node = *node;
        *node = temp;
        index = 1;
    }

    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    int neighbor_node_key_num = (*neighbor_node)->GetSize();
    int node_key_num = (*node)->GetSize();
    for (int i = 0; i < node_key_num; i++) {
        (*neighbor_node)->insert_pair(neighbor_node_key_num + i, (*node)->get_key(i), *(*node)->get_rid(i));
        maintain_child(*neighbor_node, neighbor_node_key_num + i);
    }

    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    if ((*node)->IsLeafPage()) {
        if ((*node)->GetPageNo() == file_hdr_.last_leaf) {
            file_hdr_.last_leaf = (*neighbor_node)->GetPageNo();
        }
        (*neighbor_node)->page_hdr->next_leaf = (*node)->page_hdr->next_leaf;
        IxNodeHandle *nextnode = FetchNode((*node)->page_hdr->next_leaf);
        nextnode->page_hdr->prev_leaf = (*neighbor_node)->GetPageNo();
    }
    release_node_handle(**node);  // BUG
    delete *node;                 // BUG 为啥没有析构函数?这样写可以吗?底层的page怎么办?
    (*parent)->erase_pair(index);
    (*parent)->set_key(index - 1, (*neighbor_node)->get_key(0));
    (*parent)->set_rid(index - 1, Rid{(*neighbor_node)->GetPageNo(), -1});
    maintain_child(*parent, index - 1);
    maintain_parent(*neighbor_node);
    return CoalesceOrRedistribute(*parent, transaction);
}

/** -- 以下为辅助函数 -- */
/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::FetchNode(int page_no) const {
    // assert(page_no < file_hdr_.num_pages); // 不再生效，由于删除操作，page_no可以大于个数
    Page *page = buffer_pool_manager_->FetchPage(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(&file_hdr_, page);
    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::CreateNode() {
    file_hdr_.num_pages++;
    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->NewPage(&new_page_id);
    // 注意，和Record的free_page定义不同，此处【不能】加上：file_hdr_.first_free_page_no = page->GetPageId().page_no
    IxNodeHandle *node = new IxNodeHandle(&file_hdr_, page);
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->GetParentPageNo() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = FetchNode(curr->GetParentPageNo());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        // char *child_max_key = curr.get_key(curr.page_hdr->num_key - 1);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_.col_len) == 0) {
            assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_.col_len);  // 修改了parent node
        curr = parent;

        assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->IsLeafPage());

    IxNodeHandle *prev = FetchNode(leaf->GetPrevLeaf());
    prev->SetNextLeaf(leaf->GetNextLeaf());
    buffer_pool_manager_->UnpinPage(prev->GetPageId(), true);

    IxNodeHandle *next = FetchNode(leaf->GetNextLeaf());
    next->SetPrevLeaf(leaf->GetPrevLeaf());  // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->UnpinPage(next->GetPageId(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) { file_hdr_.num_pages--; }

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->IsLeafPage()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->ValueAt(child_idx);
        IxNodeHandle *child = FetchNode(child_page_no);
        child->SetParentPageNo(node->GetPageNo());
        buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    }
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = FetchNode(iid.page_no);
    if (iid.slot_no >= node->GetSize()) {
        throw IndexEntryNotFoundError();
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // unpin it!
    return *node->get_rid(iid.slot_no);
}

/** --以下函数将用于lab3执行层-- */
/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key) {
    // int int_key = *(int *)key;
    // printf("my_lower_bound key=%d\n", int_key);

    IxNodeHandle *node = FindLeafPage(key, Operation::FIND, nullptr);
    int key_idx = node->lower_bound(key);

    Iid iid = {.page_no = node->GetPageNo(), .slot_no = key_idx};

    // unpin leaf node
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return iid;
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key) {
    // int int_key = *(int *)key;
    // printf("my_upper_bound key=%d\n", int_key);

    IxNodeHandle *node = FindLeafPage(key, Operation::FIND, nullptr);
    int key_idx = node->upper_bound(key);

    Iid iid;
    if (key_idx == node->GetSize()) {
        // 这种情况无法根据iid找到rid，即后续无法调用ih->get_rid(iid)
        iid = leaf_end();
    } else {
        iid = {.page_no = node->GetPageNo(), .slot_no = key_idx};
    }

    // unpin leaf node
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_.first_leaf, .slot_no = 0};
    return iid;
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = FetchNode(file_hdr_.last_leaf);
    Iid iid = {.page_no = file_hdr_.last_leaf, .slot_no = node->GetSize()};
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // unpin it!
    return iid;
}
