#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        // Get all index files
        std::vector<IxIndexHandle *> ihs(tab_.cols.size(), nullptr);
        for (size_t col_i = 0; col_i < tab_.cols.size(); col_i++) {
            if (tab_.cols[col_i].index) {
                // lab3 task3 Todo
                // 获取需要的索引句柄,填充vector ihs
                // lab3 task3 Todo end
                ihs[col_i] = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, col_i)).get();
            }
            else ihs[col_i]=nullptr;
        }
        // Delete each rid from record file and index file
        for (auto &rid : rids_) {
            auto rec = fh_->get_record(rid, context_);
            // lab3 task3 Todo
            // Delete from index file
            // Delete from record file
            // lab3 task3 Todo end
            
            // Delete from index file
            for (long unsigned int i = 0; i < tab_.cols.size(); i++) {
                if (tab_.cols[i].index) {
                    auto ifh = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, i)).get();;//index file handle
                    ifh->delete_entry(rec->data + tab_.cols[i].offset,context_->txn_);
                }
            }
            // Delete from record file
            fh_->delete_record(rid, context_);

            // record a delete operation into the transaction
            WriteRecord* wr= new WriteRecord(WType::DELETE_TUPLE, tab_name_,rid, *rec);
            context_->txn_->AppendWriteRecord(wr);  
        }
        return nullptr;
    }
    Rid &rid() override { return _abstract_rid; }
};