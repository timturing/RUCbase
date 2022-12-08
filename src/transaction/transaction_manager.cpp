#include "transaction_manager.h"

#include "record/rm_file_handle.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * 事务的开始方法
 * @param txn 事务指针
 * @param log_manager 日志管理器，用于日志lab
 * @return 当前事务指针
 * @tips: 事务的指针可能为空指针
 */
Transaction *TransactionManager::Begin(Transaction *txn, LogManager *log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_);
        txn_map[txn->GetTransactionId()] = txn;
        next_txn_id_++;  // TODO只有这里有吗
    } else {
        txn_map[txn->GetTransactionId()] = txn;
    }
    // std::cout<<txn->GetTransactionId()<<" begin"<<std::endl;
    return txn;
}

/**
 * 事务的提交方法
 * @param txn 事务指针
 * @param log_manager 日志管理器，用于日志lab
 * @param sm_manager 系统管理器，用于commit，后续会删掉
 */
void TransactionManager::Commit(Transaction *txn, LogManager *log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 更新事务状态

    // 1. 如果存在未提交的写操作，提交所有的写操作
    if (txn->GetWriteSet()->size() != 0) {
        for (auto it = txn->GetWriteSet()->begin(); it != txn->GetWriteSet()->end(); it++) {
            log_manager->AppendLogRecord(
                new LogRecord(txn->GetTransactionId(), txn->GetPrevLsn(), LogRecordType::COMMIT));
            // next_txn_id_--;
            //?提交
        }
    }
    // 2. 释放所有锁
    for (auto it = txn->GetLockSet()->begin(); it != txn->GetLockSet()->end(); it++) {
        lock_manager_->Unlock(txn, *it);
    }
    // 3. 释放事务相关资源，eg.锁集
    txn->GetLockSet()->clear();
    txn->GetWriteSet()->clear();
    // 4. 更新事务状态
    // std::cout<<txn->GetTransactionId()<<" commit"<<std::endl;
    txn->SetState(TransactionState::COMMITTED);
    
}

/**
 * 事务的终止方法
 * @param txn 事务指针
 * @param log_manager 日志管理器，用于日志lab
 * @param sm_manager 系统管理器，用于rollback，后续会删掉
 */
void TransactionManager::Abort(Transaction *txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 更新事务状态

    // 1. 回滚所有写操作
    if (txn->GetWriteSet()->size() != 0) {
        for (auto it = txn->GetWriteSet()->begin(); it != txn->GetWriteSet()->end(); it++) {
            log_manager->AppendLogRecord(
                new LogRecord(txn->GetTransactionId(), txn->GetPrevLsn(), LogRecordType::ABORT));
            // 回滚操作
            // TODO: 这里的index根本就无法回滚,尤其是update
            if ((*it)->GetWriteType() == WType::INSERT_TUPLE) {
                // 删除
                // sm_manager_->rollback_insert(table_name,rid,);//?没有这个
                Rid rid = (*it)->GetRid();
                std::string tab_name = (*it)->GetTableName();
                auto tab_ = sm_manager_->db_.get_table(tab_name);
                auto fh_ = sm_manager_->fhs_.at(tab_name).get();
                Context *context_ = new Context(lock_manager_, log_manager, txn);
                auto rec = fh_->get_record(rid, context_);
                // delete index
                for (int i = 0; i < tab_.cols.size(); i++) {
                    if (tab_.cols[i].index) {
                        auto ifh = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, i))
                                       .get();  // index file handle
                        ifh->delete_entry(rec->data + tab_.cols[i].offset, context_->txn_);
                    }
                    // delete record
                    fh_->delete_record(rid, context_);
                }
            } else if ((*it)->GetWriteType() == WType::UPDATE_TUPLE) {
                // 更新
                Rid rid = (*it)->GetRid();
                std::string tab_name = (*it)->GetTableName();
                RmRecord rec = (*it)->GetRecord();
                auto tab_ = sm_manager_->db_.get_table(tab_name);
                auto fh_ = sm_manager_->fhs_.at(tab_name).get();
                Context *context_ = new Context(lock_manager_, log_manager, txn);
                // delete index
                for (int i = 0; i < tab_.cols.size(); i++) {
                    if (tab_.cols[i].index) {
                        auto ifh = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, i))
                                       .get();  // index file handle
                        ifh->delete_entry(rec.data + tab_.cols[i].offset, context_->txn_);
                    }
                    // update record
                    fh_->update_record(rid, rec.data, context_);
                    // insert index
                    for (int i = 0; i < tab_.cols.size(); i++) {
                        if (tab_.cols[i].index) {
                            auto ifh = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, i))
                                           .get();  // index file handle
                            ifh->insert_entry(rec.data + tab_.cols[i].offset, rid, context_->txn_);
                        }
                    }
                }
            } else if ((*it)->GetWriteType() == WType::DELETE_TUPLE) {
                // 插入
                Rid rid = (*it)->GetRid();
                std::string tab_name = (*it)->GetTableName();
                RmRecord rec = (*it)->GetRecord();
                auto tab_ = sm_manager_->db_.get_table(tab_name);
                auto fh_ = sm_manager_->fhs_.at(tab_name).get();
                Context *context_ = new Context(lock_manager_, log_manager, txn);
                // insert index
                for (size_t i = 0; i < tab_.cols.size(); i++) {
                    auto &col = tab_.cols[i];
                    if (col.index) {
                        auto ih =
                            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, i)).get();
                        ih->insert_entry(rec.data + col.offset, rid, context_->txn_);
                    }
                }
                // insert record
                fh_->insert_record(rid, rec.data);
            }
        }
    }
    // 2. 释放所有锁
    for (auto it = txn->GetLockSet()->begin(); it != txn->GetLockSet()->end(); it++) {
        lock_manager_->Unlock(txn, *it);
    }
    // 3. 清空事务相关资源，eg.锁集
    txn->GetLockSet()->clear();
    txn->GetWriteSet()->clear();
    // 4. 更新事务状态
    // std::cout<<txn->GetTransactionId()<<" abort"<<std::endl;
    txn->SetState(TransactionState::ABORTED);
}

/** 以下函数用于日志实验中的checkpoint */
void TransactionManager::BlockAllTransactions() {}

void TransactionManager::ResumeAllTransactions() {}