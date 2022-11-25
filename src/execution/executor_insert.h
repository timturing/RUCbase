#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Value> values_;
    RmFileHandle *fh_;
    std::string tab_name_;
    Rid rid_;
    SmManager *sm_manager_;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        // Get record file handle
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // lab3 task3 Todo
        // Make record buffer
        // Insert into record file
        // Insert into index
        // lab3 task3 Todo end

        // TODO 这里的transation为啥没有???
        txn_id_t txn_id;
        IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE;
        Transaction temp(txn_id, isolation_level);
        // Make record buffer
        std::unique_ptr<RmRecord> record{new RmRecord(fh_->get_file_hdr().record_size)};

        // Insert into record file
        rid_ = fh_->insert_record(record->data,context_);
        // Insert into index
        for (int i = 0; i < tab_.cols.size(); i++) {
            // memcpy(record->data + tab_.cols[i].offset, values_[i].raw->data, values_[i].raw->size);
            auto val = values_[i];
            if (val.type == TYPE_INT) {
                memcpy(record->data + tab_.cols[i].offset, &val.int_val, sizeof(int));
            } else if (val.type == TYPE_FLOAT) {
                memcpy(record->data + tab_.cols[i].offset, &val.float_val, sizeof(float));
            } else if (val.type == TYPE_STRING) {
                memcpy(record->data + tab_.cols[i].offset, val.str_val.c_str(), val.str_val.size());
            } 
            if (tab_.cols[i].index) {
                auto ifh = sm_manager_->ihs_.at(tab_name_).get();  // index file handle
                ifh->insert_entry(record->data + tab_.cols[i].offset, rid_, &temp);
            }
        }
        fh_->update_record(rid_, record->data,context_);
        // printf("end\n");
        return record;
    }
    Rid &rid() override { return rid_; }
};