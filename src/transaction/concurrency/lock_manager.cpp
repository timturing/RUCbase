#include "lock_manager.h"

/**
 * 申请行级读锁
 * @param txn 要申请锁的事务对象指针
 * @param rid 加锁的目标记录ID
 * @param tab_fd 记录所在的表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockSharedOnRecord(Transaction *txn, const Rid &rid, int tab_fd) {
    // Todo:
    // 1. 通过mutex申请访问全局锁表
    // 2. 检查事务的状态
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示

    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    auto lock_data_id = LockDataId(tab_fd, rid, LockDataType::RECORD);
    auto lock_data = lock_table_.find(lock_data_id);
    if (lock_data != lock_table_.end()) {
        auto request_queue = lock_data->second.request_queue_;
        for(auto request : request_queue) {
            // 3.1 如果当前事务已经申请了目标数据项上的锁，且锁类型为S锁，则直接返回true
            if (request.txn_id_ == txn->GetTransactionId() && request.lock_mode_ == LockMode::SHARED) {
                //TODO 是否需要request.granted_ = true; && 不要忘记先更改后push
                return true;
            }
            // 3.2 如果当前事务已经申请了目标数据项上的锁，且锁类型为X锁，则直接返回false
            if (request.txn_id_ == txn->GetTransactionId() && request.lock_mode_ == LockMode::EXLUCSIVE) {
                //TODO 是false吗
                return false;
            }
        }
    }

    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    LockRequest request(txn->GetTransactionId(), LockMode::SHARED);
    txn->SetState(TransactionState::GROWING);
    // 4.1 如果当前数据项在全局锁表中不存在，则直接将锁申请放入到全局锁表中
    if (lock_data == lock_table_.end()) {
        lock_table_[lock_data_id].request_queue_.push_back(request);
        lock_table_[lock_data_id].group_lock_mode_ = GroupLockMode::S;
        
        return true;
    }
    // 4.2 如果当前数据项在全局锁表中存在，则判断当前数据项的组模式
    else {
        // 4.2.1 如果当前数据项的组模式为非锁模式，则直接将锁申请放入到全局锁表中
        if (lock_data->second.group_lock_mode_ == GroupLockMode::NON_LOCK) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            lock_data->second.group_lock_mode_ = GroupLockMode::S;
            
            return true;
        }
        // 4.2.2 如果当前数据项的组模式为S锁模式，则直接将锁申请放入到全局锁表中
        if (lock_data->second.group_lock_mode_ == GroupLockMode::S) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            
            return true;
        }
        // 4.2.3 如果当前数据项的组模式为X锁模式，则将锁申请放入到全局锁表中，并阻塞当前操作
        if (lock_data->second.group_lock_mode_ == GroupLockMode::X) {
            request.granted_ = false;
            lock_data->second.request_queue_.push_back(request);
            // 通过条件变量来阻塞当前操作
            while(lock_data->second.group_lock_mode_ != GroupLockMode::S&&lock_data->second.group_lock_mode_ != GroupLockMode::NON_LOCK)
            lock_data->second.cv_.wait(lock);

            for(auto request : lock_data->second.request_queue_) {
                if (request.txn_id_ == txn->GetTransactionId()) {
                    request.granted_ = true;
                    break;
                }
            }
            lock_data->second.group_lock_mode_ = GroupLockMode::S;
            return true;
        }
    }
    return false;
}

/**
 * 申请行级写锁
 * @param txn 要申请锁的事务对象指针
 * @param rid 加锁的目标记录ID
 * @param tab_fd 记录所在的表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockExclusiveOnRecord(Transaction *txn, const Rid &rid, int tab_fd) {
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    auto lock_data_id = LockDataId(tab_fd, rid, LockDataType::RECORD);
    auto lock_data = lock_table_.find(lock_data_id);
    if (lock_data != lock_table_.end()) {
        auto request_queue = lock_data->second.request_queue_;
        for(auto request : request_queue) {
            if (request.txn_id_ == txn->GetTransactionId()) {
                if (request.lock_mode_ == LockMode::SHARED) {
                    // 如果当前事务已经申请了目标数据项上的S锁，则将其转换为X锁
                    //TODO ?如果其他事务有S锁，则不能申请X锁，需要等待其他事务释放S锁
                    request.lock_mode_ = LockMode::EXLUCSIVE;
                    return true;
                }
                if (request.lock_mode_ == LockMode::EXLUCSIVE) {
                    // 如果当前事务已经申请了目标数据项上的X锁，则直接返回true
                    return true;
                }
            }
        }
    }

    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    LockRequest request(txn->GetTransactionId(), LockMode::EXLUCSIVE);
    txn->SetState(TransactionState::GROWING);
    // 4.1 如果当前数据项在全局锁表中不存在，则直接将锁申请放入到全局锁表中
    if (lock_data == lock_table_.end()) {
        request.granted_ = true;
        lock_table_[lock_data_id].request_queue_.push_back(request);
        lock_table_[lock_data_id].group_lock_mode_ = GroupLockMode::X;
        
        return true;
    }
    // 4.2 如果当前数据项在全局锁表中存在，则判断当前数据项的组模式
    else {
        // 4.2.1 如果当前数据项的组模式为非锁模式，则直接将锁申请放入到全局锁表中
        if (lock_data->second.group_lock_mode_ == GroupLockMode::NON_LOCK) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            lock_data->second.group_lock_mode_ = GroupLockMode::X;
            
            return true;
        }

        // 4.2.2 否则都阻塞
        request.granted_ = false;
        lock_data->second.request_queue_.push_back(request);
        // 通过条件变量来阻塞当前操作
        while(lock_data->second.group_lock_mode_ != GroupLockMode::NON_LOCK)
        lock_data->second.cv_.wait(lock);

        for(auto request : lock_data->second.request_queue_) {
            if (request.txn_id_ == txn->GetTransactionId()) {
                request.granted_ = true;
                break;
            }
        }
        lock_data->second.group_lock_mode_ = GroupLockMode::X;
        return true;
    }
    return false;
}

/**
 * 申请表级读锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockSharedOnTable(Transaction *txn, int tab_fd) {
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto lock_data = lock_table_.find(lock_data_id);
    if (lock_data != lock_table_.end()) {
        auto request_queue = lock_data->second.request_queue_;
        for(auto request : request_queue) {
            if (request.txn_id_ == txn->GetTransactionId()) {
                if (request.lock_mode_ == LockMode::SHARED) {
                    return true;
                }
                else {
                    return false;
                }
            }
        }
    }

    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    LockRequest request(txn->GetTransactionId(), LockMode::SHARED);
    txn->SetState(TransactionState::GROWING);
    // 4.1 如果当前数据项在全局锁表中不存在，则直接将锁申请放入到全局锁表中
    if (lock_data == lock_table_.end()) {
        request.granted_ = true;
        lock_table_[lock_data_id].request_queue_.push_back(request);
        lock_table_[lock_data_id].group_lock_mode_ = GroupLockMode::S;
        
        return true;
    }
    // 4.2 如果当前数据项在全局锁表中存在，则判断当前数据项的组模式
    else {
        // 4.2.1 如果当前数据项的组模式为非锁模式，则直接将锁申请放入到全局锁表中
        if (lock_data->second.group_lock_mode_ == GroupLockMode::NON_LOCK) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            lock_data->second.group_lock_mode_ = GroupLockMode::S;
            return true;
        }
        // 4.2.2 如果当前数据项的组模式为S锁模式，则直接将锁申请放入到全局锁表中
        if (lock_data->second.group_lock_mode_ == GroupLockMode::S) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            return true;
        }
        // 4.2.3 如果是IS锁(因为IS与S锁是兼容的，所以可以直接申请S锁)
        if (lock_data->second.group_lock_mode_ == GroupLockMode::IS) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            lock_data->second.group_lock_mode_ = GroupLockMode::S;
            return true;
        }
        request.granted_ = false;
        lock_data->second.request_queue_.push_back(request);
        // 通过条件变量来阻塞当前操作
        while(lock_data->second.group_lock_mode_ != GroupLockMode::NON_LOCK&&lock_data->second.group_lock_mode_ != GroupLockMode::S&&lock_data->second.group_lock_mode_ != GroupLockMode::IS)
        lock_data->second.cv_.wait(lock);

        for(auto request : lock_data->second.request_queue_) {
            if (request.txn_id_ == txn->GetTransactionId()) {
                request.granted_ = true;
                break;
            }
        }
        lock_data->second.group_lock_mode_ = GroupLockMode::S;
        return true;
        
    }
    return false;
}

/**
 * 申请表级写锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockExclusiveOnTable(Transaction *txn, int tab_fd) {
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    // std::cout<<lock_data_id.fd_<<std::endl;
    auto lock_data = lock_table_.find(lock_data_id);
    if (lock_data != lock_table_.end()) {
        auto request_queue = lock_data->second.request_queue_;
        for(auto request : request_queue) {
            if (request.txn_id_ == txn->GetTransactionId()) {
                if (request.lock_mode_ == LockMode::SHARED) {
                    // 如果当前事务已经申请了目标数据项上的S锁，则将其转换为X锁
                    request.lock_mode_ = LockMode::EXLUCSIVE;
                    return true;
                }
                if (request.lock_mode_ == LockMode::EXLUCSIVE) {
                    // 如果当前事务已经申请了目标数据项上的X锁，则直接返回true
                    return true;
                }
            }
        }
    }

    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    LockRequest request(txn->GetTransactionId(), LockMode::EXLUCSIVE);
    txn->SetState(TransactionState::GROWING);
    // 4.1 如果当前数据项在全局锁表中不存在，则直接将锁申请放入到全局锁表中
    if (lock_data == lock_table_.end()) {
        request.granted_ = true;
        lock_table_[lock_data_id].request_queue_.push_back(request);
        lock_table_[lock_data_id].group_lock_mode_ = GroupLockMode::X;
        // std::cout<<"insert lock request into lock table, this table not added lock before"<<std::endl;
        return true;
    }
    // 4.2 如果当前数据项在全局锁表中存在，则判断当前数据项的组模式
    else {
        // 4.2.1 如果当前数据项的组模式为非锁模式，则直接将锁申请放入到全局锁表中
                // std::cout<<"mode: "<<lock_data->second.group_lock_mode_<<std::endl;
        if (lock_data->second.group_lock_mode_ == GroupLockMode::NON_LOCK) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            lock_data->second.group_lock_mode_ = GroupLockMode::X;
            return true;
        }
        // 4.2.2 否则都阻塞
        request.granted_ = false;
        lock_data->second.request_queue_.push_back(request);
        // 通过条件变量来阻塞当前操作
        while(lock_data->second.group_lock_mode_ != GroupLockMode::NON_LOCK)
        lock_data->second.cv_.wait(lock);

        for(auto request : lock_data->second.request_queue_) {
            if (request.txn_id_ == txn->GetTransactionId()) {
                request.granted_ = true;
                break;
            }
        }
        lock_data->second.group_lock_mode_ = GroupLockMode::X;
        return true;
        
    }
    return false;
}

/**
 * 申请表级意向读锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockISOnTable(Transaction *txn, int tab_fd) {
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto lock_data = lock_table_.find(lock_data_id);
    if (lock_data != lock_table_.end()) {
        auto request_queue = lock_data->second.request_queue_;
        for(auto request : request_queue) {
            if (request.txn_id_ == txn->GetTransactionId()) {
                //如果存在IS锁，则直接返回true
                if (request.lock_mode_ == LockMode::INTENTION_SHARED) {
                    return true;
                }
                else{
                    return false;//有更强的锁
                }

            }
        }
    }

    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    LockRequest request(txn->GetTransactionId(), LockMode::INTENTION_SHARED);
    txn->SetState(TransactionState::GROWING);
    // 4.1 如果当前数据项在全局锁表中不存在，则直接将锁申请放入到全局锁表中
    if (lock_data == lock_table_.end()) {
        request.granted_ = true;
        lock_table_[lock_data_id].request_queue_.push_back(request);
        lock_table_[lock_data_id].group_lock_mode_ = GroupLockMode::IS;
        return true;
    }
    // 4.2 如果当前数据项在全局锁表中存在，则判断当前数据项的组模式
    else {
        // 4.2.1 如果当前数据项的组模式为非锁模式，则直接将锁申请放入到全局锁表中
        if (lock_data->second.group_lock_mode_ == GroupLockMode::NON_LOCK) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            lock_data->second.group_lock_mode_ = GroupLockMode::IS;
            return true;
        }
        // 4.2.2 如果当前数据项的组模式为X
        if (lock_data->second.group_lock_mode_ == GroupLockMode::X)
        {
            // 阻塞
            request.granted_ = false;
            // 通过条件变量来阻塞当前操作
            while(lock_data->second.group_lock_mode_ == GroupLockMode::X)
            lock_data->second.cv_.wait(lock);

            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            lock_data->second.group_lock_mode_ = GroupLockMode::IS;
            return true;
        }
        //其他的是兼容的高级锁，不需要改组模式
        request.granted_ = true;
        lock_data->second.request_queue_.push_back(request);
        return true;
            
    }
    return false;
    
}

/**
 * 申请表级意向写锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockIXOnTable(Transaction *txn, int tab_fd) {
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock(latch_);
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) {
        return false;
    }
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    auto lock_data_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto lock_data = lock_table_.find(lock_data_id);
    if (lock_data != lock_table_.end()) {
        auto request_queue = lock_data->second.request_queue_;
        for(auto request : request_queue) {
            if (request.txn_id_ == txn->GetTransactionId()) {
                //如果存在IX锁，则直接返回true
                if (request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
                    return true;
                }
                else{
                    return false;
                }
            }
        }
    }

    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    LockRequest request(txn->GetTransactionId(), LockMode::INTENTION_EXCLUSIVE);
    txn->SetState(TransactionState::GROWING);
    // 4.1 如果当前数据项在全局锁表中不存在，则直接将锁申请放入到全局锁表中
    if (lock_data == lock_table_.end()) {
        request.granted_ = true;
        lock_table_[lock_data_id].request_queue_.push_back(request);
        lock_table_[lock_data_id].group_lock_mode_ = GroupLockMode::IX;
        return true;
    }
    // 4.2 如果当前数据项在全局锁表中存在，则判断当前数据项的组模式
    else {
        // 4.2.1 如果当前数据项的组模式为非锁模式，则直接将锁申请放入到全局锁表中
        if (lock_data->second.group_lock_mode_ == GroupLockMode::NON_LOCK) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            lock_data->second.group_lock_mode_ = GroupLockMode::IX;
            return true;
        }
        // 4.2.2 如果当前数据项的组模式为IX锁模式，则直接将锁申请放入到全局锁表中
        if (lock_data->second.group_lock_mode_ == GroupLockMode::IX) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            return true;
        }
        // 4.2.3 如果当前数据项的组模式为IS锁模式，则直接将锁申请放入到全局锁表中
        if (lock_data->second.group_lock_mode_ == GroupLockMode::IS) {
            request.granted_ = true;
            lock_data->second.request_queue_.push_back(request);
            lock_data->second.group_lock_mode_ = GroupLockMode::IX;
            return true;
        }
        // 其他情况，阻塞
        request.granted_ = false;
        lock_data->second.request_queue_.push_back(request);
        // 通过条件变量来阻塞当前操作
        while(lock_data->second.group_lock_mode_ != GroupLockMode::NON_LOCK&&lock_data->second.group_lock_mode_ != GroupLockMode::IS&&lock_data->second.group_lock_mode_ != GroupLockMode::IX)
        lock_data->second.cv_.wait(lock);

        for(auto request : lock_data->second.request_queue_) {
            if (request.txn_id_ == txn->GetTransactionId()) {
                request.granted_ = true;
                break;
            }
        }
        lock_data->second.group_lock_mode_ = GroupLockMode::IX;
        return true;
        
    }
    return false;
}

/**
 * 释放锁
 * @param txn 要释放锁的事务对象指针
 * @param lock_data_id 要释放的锁ID
 * @return 返回解锁是否成功
 */
bool LockManager::Unlock(Transaction *txn, LockDataId lock_data_id) {
    //更新锁表
    std::unique_lock<std::mutex> lock(latch_);
    auto lock_data = lock_table_.find(lock_data_id);
    if (lock_data == lock_table_.end()) {
        return false;
    }
    // bool modebit[6]={false};
    auto request_queue = lock_data->second.request_queue_;
    for (auto request = request_queue.begin(); request != request_queue.end(); request++) {
        if (request->txn_id_ == txn->GetTransactionId()) {
            request_queue.erase(request);
            break;
        }
        else
        {
            // if(request->lock_mode_==LockMode::INTENTION_SHARED)modebit[0]=1;
            // if(request->lock_mode_==LockMode::SHARED)modebit[1]=1;
            // if(request->lock_mode_==LockMode::INTENTION_EXCLUSIVE)modebit[2]=1;
            // if(request->lock_mode_==LockMode::S_IX)modebit[3]=1;
            // if(request->lock_mode_==LockMode::EXLUCSIVE)modebit[4]=1;
        }
    }
    // if(modebit[4])
    // {
    //     lock_data->second.group_lock_mode_ = GroupLockMode::X;
    // }
    // else if(modebit[3])
    // {
    //     lock_data->second.group_lock_mode_ = GroupLockMode::SIX;
    // }
    // else if(modebit[2])
    // {
    //     lock_data->second.group_lock_mode_ = GroupLockMode::IX;
    // }
    // else if(modebit[1])
    // {
    //     lock_data->second.group_lock_mode_ = GroupLockMode::S;
    // }
    // else if(modebit[0])
    // {
    //     lock_data->second.group_lock_mode_ = GroupLockMode::IS;
    // }
    // else 
    // {
        lock_data->second.group_lock_mode_ = GroupLockMode::NON_LOCK;//说明现在空了出来，可以让别人拿锁
    // }

    lock_table_[lock_data_id].cv_.notify_one();//避免锁竞争，一次只放一个
    if (request_queue.empty()) {
        lock_table_.erase(lock_data_id);
    }
    txn->SetState(TransactionState::SHRINKING);
    return true;
}