#include "ix_node_handle.h"

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int num_key = this->page_hdr->num_key;
    int key_idx = 0;
    if(binary_search)
    {
        //迭代式二分
        int first = 0,last=num_key-1,middle;
        char *src;
        while(first<last)
        {
            middle = (first+last)/2;
            src = get_key(middle);
            if(ix_compare(src,target,file_hdr->col_type,file_hdr->col_len)<0)
            {
                first=middle+1;
                key_idx=first;
            }
            else
            {
                last=middle;
                key_idx=last;
            }
        }
        if(key_idx==num_key-1)
        {
            if(ix_compare(get_key(num_key-1),target,file_hdr->col_type,file_hdr->col_len)<0)
            {
                return num_key;
            }
        }
    }
    else
    {
        for(key_idx=0;key_idx<num_key;key_idx++)
        {
            char* src = get_key(key_idx);
            if(ix_compare(src,target,file_hdr->col_type,file_hdr->col_len)>=0)
            {
                return key_idx;
            }
        }
    }
    return key_idx;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式：顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int num_key = this->page_hdr->num_key;
    int key_idx = 0;
    if(binary_search)
    {
        //迭代式二分
        int first = 0,last=num_key-1,middle;
        char *src;
        while(first<last)
        {
            middle = (first+last)/2;
            src = get_key(middle);
            if(ix_compare(src,target,file_hdr->col_type,file_hdr->col_len)<=0)
            {
                first=middle+1;
                key_idx=first;
            }
            else
            {
                last=middle;
                key_idx=last;
            }
        }
        if(key_idx==num_key-1)
        {
            if(ix_compare(get_key(num_key-1),target,file_hdr->col_type,file_hdr->col_len)<=0)
            {
                return num_key;
            }
        }
    }
    else
    {
        for(key_idx=0;key_idx<num_key;key_idx++)
        {
            char* src = get_key(key_idx);
            if(ix_compare(src,target,file_hdr->col_type,file_hdr->col_len)>0)
            {
                return key_idx;
            }
        }
    }
    return key_idx;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::LeafLookup(const char *key, Rid **value) {
    // Todo:
    // 1. 在叶子节点中获取目标key所在位置
    // 2. 判断目标key是否存在
    // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
    // 提示：可以调用lower_bound()和get_rid()函数。
    int key_idx = lower_bound(key);
    if(key_idx==this->page_hdr->num_key)
    {
        return false;
    } 
    char* src = get_key(key_idx);
    if(ix_compare(src,key,file_hdr->col_type,file_hdr->col_len)!=0)
    {
        return false;
    }
    *value = get_rid(key_idx);
    return true;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::InternalLookup(const char *key) {
    // Todo:
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // 2. 获取该孩子节点（子树）所在页面的编号
    // 3. 返回页面编号
    int rid_idx = upper_bound(key);
    if(rid_idx!=0)rid_idx-=1;//如果等于0说明所有key都比它大，其实都不用查了
    return ValueAt(rid_idx);
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    // Todo:
    // 1. 判断pos的合法性
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 4. 更新当前节点的键数量
    int num_key = page_hdr->num_key;
    if(pos<0||pos>num_key)
    {
        return;
    }
    for(int i=num_key-1;i>=pos;i--)
    {
        memmove(get_key(i+n),get_key(i),file_hdr->col_len);
        memmove(get_rid(i+n),get_rid(i),sizeof(Rid));
    }
    for(int i=0;i<n;i++)
    {
        memmove(get_key(pos+i),key+i*file_hdr->col_len,file_hdr->col_len);
        memmove(get_rid(pos+i),rid+i,sizeof(Rid));
    }
    page_hdr->num_key += n;

    // // 1.
    // if(pos<0||pos>num_key)return;
    // // 2.
    // char* keydes =  get_key(pos);
    // int keylen = n*file_hdr->col_len;
    // memmove(keydes+keylen,keydes,keylen);
    // memmove(keydes,key,keylen);
    // // 3.
    // Rid* riddes = get_rid(pos);
    // int ridlen = n*sizeof(Rid);
    // memmove(riddes+n,riddes,ridlen);
    // memmove(riddes,rid,ridlen);
    // // 4.
    // page_hdr->num_key=num_key+n;
    // return;
}

/**
 * @brief 用于在结点中的指定位置插入单个键值对
 */
void IxNodeHandle::insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); };

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::Insert(const char *key, const Rid &value) {
    // Todo:
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // 2. 如果key重复则不插入
    // 3. 如果key不重复则插入键值对
    // 4. 返回完成插入操作之后的键值对数量
    int key_idx = lower_bound(key);
    if(key_idx<page_hdr->num_key&&ix_compare(get_key(key_idx),key,file_hdr->col_type,file_hdr->col_len)==0)
    {
        return page_hdr->num_key;
    }
    insert_pair(key_idx,key,value);
    return page_hdr->num_key;
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    // Todo:
    // 1. 删除该位置的key
    // 2. 删除该位置的rid
    // 3. 更新结点的键值对数量

    int num_key = page_hdr->num_key;
    if(pos<0||pos>=num_key)
    {
        return;
    }
    memmove(get_key(pos),get_key(pos+1),(num_key-pos-1)*file_hdr->col_len);
    memmove(get_rid(pos),get_rid(pos+1),(num_key-pos-1)*sizeof(Rid));
    page_hdr->num_key -= 1;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::Remove(const char *key) {
    // Todo:
    // 1. 查找要删除键值对的位置
    // 2. 如果要删除的键值对存在，删除键值对
    // 3. 返回完成删除操作后的键值对数量

    int key_idx = lower_bound(key);
    if(key_idx<page_hdr->num_key&&ix_compare(get_key(key_idx),key,file_hdr->col_type,file_hdr->col_len)==0)
    {
        erase_pair(key_idx);
    }
    return page_hdr->num_key;
}

/**
 * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
 *
 * @param child
 * @return int
 */
int IxNodeHandle::find_child(IxNodeHandle *child) {
    int rid_idx;
    for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
        if (get_rid(rid_idx)->page_no == child->GetPageNo()) {
            break;
        }
    }
    assert(rid_idx < page_hdr->num_key);
    return rid_idx;
}

/**
 * @brief used in internal node to remove the last key in root node, and return the last child
 *
 * @return the last child
 */
page_id_t IxNodeHandle::RemoveAndReturnOnlyChild() {
    assert(GetSize() == 1);
    page_id_t child_page_no = ValueAt(0);
    erase_pair(0);
    assert(GetSize() == 0);
    return child_page_no;
}