#include "execution_manager.h"

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

TabCol QlManager::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        // Make sure target column exists
        if (!(sm_manager_->db_.is_table(target.tab_name) &&
              sm_manager_->db_.get_table(target.tab_name).is_col(target.col_name))) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
    }
    return target;
}

std::vector<ColMeta> QlManager::get_all_cols(const std::vector<std::string> &tab_names) {
    std::vector<ColMeta> all_cols;
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
    return all_cols;
}

std::vector<Condition> QlManager::check_where_clause(const std::vector<std::string> &tab_names,
                                                     const std::vector<Condition> &conds) {
    auto all_cols = get_all_cols(tab_names);
    // Get raw values in where clause
    std::vector<Condition> res_conds = conds;
    for (auto &cond : res_conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        if (lhs_type != rhs_type) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
    return res_conds;
}

int QlManager::get_indexNo(std::string tab_name, std::vector<Condition> curr_conds) {
    int index_no = -1;
    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    for (auto &cond : curr_conds) {
        if (cond.is_rhs_val && cond.op != OP_NE) {
            // If rhs is value and op is not "!=", find if lhs has index
            auto lhs_col = tab.get_col(cond.lhs_col.col_name);
            if (lhs_col->index) {
                // This column has index, use it
                index_no = lhs_col - tab.cols.begin();
                break;
            }
        }
    }
    return index_no;
}

void QlManager::insert_into(const std::string &tab_name, std::vector<Value> values, Context *context) {
    // lab3 task3 Todo
    // make InsertExecutor
    // call InsertExecutor.Next()
    // lab3 task3 Todo end
    InsertExecutor executor(sm_manager_, tab_name, values, context);
    // RmFileHandle *rfh = sm_manager_->fhs_.at(tab_name).get();
    // context->lock_mgr_->LockIXOnTable(context->txn_, rfh->GetFd());
    // LockDataId lock_data_id = LockDataId{rfh->GetFd(), LockDataType::TABLE};
    // context->txn_->GetLockSet()->insert(lock_data_id);
    executor.Next();
}

void QlManager::delete_from(const std::string &tab_name, std::vector<Condition> conds, Context *context) {
    // Parse where clause
    conds = check_where_clause({tab_name}, conds);
    // Get all RID to delete
    std::vector<Rid> rids;
    // make scan executor
    std::unique_ptr<AbstractExecutor> scanExecutor;
    // lab3 task3 Todo
    // 根据get_indexNo判断conds上有无索引
    // 创建合适的scan executor(有索引优先用索引)
    // lab3 task3 Todo end
    // RmFileHandle *rfh = sm_manager_->fhs_.at(tab_name).get();
    // context->lock_mgr_->LockIXOnTable(context->txn_, rfh->GetFd());
    // LockDataId lock_data_id = LockDataId{rfh->GetFd(), LockDataType::TABLE};
    // context->txn_->GetLockSet()->insert(lock_data_id);
    int index_no = get_indexNo(tab_name, conds);
    if (index_no == -1) {
        scanExecutor = std::make_unique<SeqScanExecutor>(sm_manager_, tab_name, conds, context);
    } else {
        scanExecutor = std::make_unique<IndexScanExecutor>(sm_manager_, tab_name, conds, index_no, context);
    }

    for (scanExecutor->beginTuple(); !scanExecutor->is_end(); scanExecutor->nextTuple()) {
        rids.push_back(scanExecutor->rid());
    }

    // lab3 task3 Todo
    // make deleteExecutor
    // call deleteExecutor.Next()
    // lab3 task3 Todo end
    DeleteExecutor executor(sm_manager_, tab_name, conds, rids, context);
    executor.Next();
}

void QlManager::update_set(const std::string &tab_name, std::vector<SetClause> set_clauses,
                           std::vector<Condition> conds, Context *context) {
    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    // Parse where clause
    conds = check_where_clause({tab_name}, conds);
    // Get raw values in set clause
    for (auto &set_clause : set_clauses) {
        auto lhs_col = tab.get_col(set_clause.lhs.col_name);
        if (lhs_col->type != set_clause.rhs.type) {
            throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(set_clause.rhs.type));
        }
        set_clause.rhs.init_raw(lhs_col->len);
    }
    // Get all RID to update
    std::vector<Rid> rids;

    // lab3 task3 Todo
    // make scan executor
    // for (scanExecutor->beginTuple(); !scanExecutor->is_end(); scanExecutor->nextTuple())
    // 将rid存入rids
    // make updateExecutor
    // call updateExecutor.Next()
    // lab3 task3 Todo end
    // RmFileHandle *rfh = sm_manager_->fhs_.at(tab_name).get();
    // context->lock_mgr_->LockIXOnTable(context->txn_, rfh->GetFd());
    // LockDataId lock_data_id = LockDataId{rfh->GetFd(), LockDataType::TABLE};
    // context->txn_->GetLockSet()->insert(lock_data_id);
    std::unique_ptr<AbstractExecutor> scanExecutor;
    int index_no = get_indexNo(tab_name, conds);
    if (index_no == -1) {
        scanExecutor = std::make_unique<SeqScanExecutor>(sm_manager_, tab_name, conds, context);
    } else {
        scanExecutor = std::make_unique<IndexScanExecutor>(sm_manager_, tab_name, conds, index_no, context);
    }
    for (scanExecutor->beginTuple(); !scanExecutor->is_end(); scanExecutor->nextTuple()) {
        rids.push_back(scanExecutor->rid());
    }
    UpdateExecutor executor(sm_manager_, tab_name, set_clauses, conds, rids, context);
    executor.Next();
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::vector<Condition> pop_conds(std::vector<Condition> &conds, const std::vector<std::string> &tab_names) {
    auto has_tab = [&](const std::string &tab_name) {
        return std::find(tab_names.begin(), tab_names.end(), tab_name) != tab_names.end();
    };
    std::vector<Condition> solved_conds;
    auto it = conds.begin();
    while (it != conds.end()) {
        if (has_tab(it->lhs_col.tab_name) && (it->is_rhs_val || has_tab(it->rhs_col.tab_name))) {
            solved_conds.emplace_back(std::move(*it));
            it = conds.erase(it);
        } else {
            it++;
        }
    }
    return solved_conds;
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
void QlManager::select_from(std::vector<TabCol> sel_cols, const std::vector<std::string> &tab_names,
                            std::vector<Condition> conds, Context *context) {
    // Parse selector
    auto all_cols = get_all_cols(tab_names);
    if (sel_cols.empty()) {
        // select all columns
        for (auto &col : all_cols) {
            TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
            sel_cols.push_back(sel_col);
        }
    } else {
        // infer table name from column name
        for (auto &sel_col : sel_cols) {
            sel_col = check_column(all_cols, sel_col);  //列元数据校验
        }
    }
    // Parse where clause
    conds = check_where_clause(tab_names, conds);
    // Scan table , 生成表算子列表tab_nodes
    std::vector<std::unique_ptr<AbstractExecutor>> table_scan_executors(tab_names.size());
    for (size_t i = 0; i < tab_names.size(); i++) {
        auto curr_conds = pop_conds(conds, {tab_names.begin(), tab_names.begin() + i + 1});
        int index_no = get_indexNo(tab_names[i], curr_conds);
        // lab3 task2 Todo
        // 根据get_indexNo判断conds上有无索引
        // 创建合适的scan executor(有索引优先用索引)存入table_scan_executors
        // lab3 task2 Todo end
        // for (std::string tab_name : tab_names) {
        //     RmFileHandle *rfh = sm_manager_->fhs_.at(tab_name).get();
        //     context->lock_mgr_->LockISOnTable(context->txn_, rfh->GetFd());
        //     LockDataId lock_data_id = LockDataId{rfh->GetFd(), LockDataType::TABLE};
        //     context->txn_->GetLockSet()->insert(lock_data_id);
        // }
        if (index_no != -1) {
            // printf("index_no: %d\n", index_no);
            table_scan_executors[i] =
                std::make_unique<IndexScanExecutor>(sm_manager_, tab_names[i], curr_conds, index_no, context);
        } else {
            // printf("no index\n");
            table_scan_executors[i] = std::make_unique<SeqScanExecutor>(sm_manager_, tab_names[i], curr_conds, context);
        }
    }
    assert(conds.empty());

    std::unique_ptr<AbstractExecutor> executorTreeRoot = std::move(table_scan_executors.back());

    // lab3 task2 Todo
    // 构建算子二叉树
    // 逆序遍历tab_nodes为左节点, 现query_plan为右节点,生成joinNode作为新query_plan 根节点
    // 生成query_plan tree完毕后, 根节点转换成投影算子
    // lab3 task2 Todo End
    for (int i = tab_names.size() - 2; i >= 0; i--) {
        std::unique_ptr<AbstractExecutor> left = std::move(table_scan_executors[i]);
        std::unique_ptr<AbstractExecutor> right = std::move(executorTreeRoot);
        executorTreeRoot = std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right));
    }
    executorTreeRoot = std::make_unique<ProjectionExecutor>(std::move(executorTreeRoot), sel_cols);

    // Column titles
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        captions.push_back(sel_col.col_name);
    }
    // Print header
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // Print records
    size_t num_rec = 0;
    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols()) {
            std::string col_str;
            char *rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                col_str = std::to_string(*(int *)rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string(*(float *)rec_buf);
            } else if (col.type == TYPE_STRING) {
                col_str = std::string((char *)rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            }
            columns.push_back(col_str);
        }
        rec_printer.print_record(columns, context);
        num_rec++;
    }
    // Print footer
    rec_printer.print_separator(context);
    // Print record count
    RecordPrinter::print_record_count(num_rec, context);
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 * @param order_cols select plan 排序的列
 * @param limit select plan 限制输出的个数
 */
void QlManager::select_from_orderby(std::vector<TabCol> sel_cols, const std::vector<std::string> &tab_names,
                            std::vector<Condition> conds, std::vector<OrderByCol> order_cols, int limit,
                            Context *context) 
{
    std::vector<int> is_asc;//排序法则
    std::vector<ColType> coltype;
     // Parse selector
    auto all_cols = get_all_cols(tab_names);
    if (sel_cols.empty()) {
        // select all columns
        for (auto &col : all_cols) {
            TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
            sel_cols.push_back(sel_col);
            is_asc.push_back(-1);
        }
    } else {
        // infer table name from column name
        for (auto &sel_col : sel_cols) {
            sel_col = check_column(all_cols, sel_col);  // 列元数据校验
            is_asc.push_back(-1);
        }
    }
    // !有重复怎么办

    for (auto &order_col : order_cols) {
        TabCol sel_col = {.tab_name ="", .col_name=order_col.col_name};
        sel_col = check_column(all_cols, sel_col);  // 列元数据校验
        order_col.tab_name = sel_col.tab_name;
        sel_cols.push_back(sel_col);
        is_asc.push_back(order_col.is_asc);
    }
    // Parse where clause
    conds = check_where_clause(tab_names, conds);
    // Scan table , 生成表算子列表tab_nodes
    std::vector<std::unique_ptr<AbstractExecutor>> table_scan_executors(tab_names.size());
    for (size_t i = 0; i < tab_names.size(); i++) {
        auto curr_conds = pop_conds(conds, {tab_names.begin(), tab_names.begin() + i + 1});
        int index_no = get_indexNo(tab_names[i], curr_conds);
        // lab3 task2 Todo
        // 根据get_indexNo判断conds上有无索引
        // 创建合适的scan executor(有索引优先用索引)存入table_scan_executors
        // lab3 task2 Todo end
        // for (std::string tab_name : tab_names) {
        //     RmFileHandle *rfh = sm_manager_->fhs_.at(tab_name).get();
        //     context->lock_mgr_->LockISOnTable(context->txn_, rfh->GetFd());
        //     LockDataId lock_data_id = LockDataId{rfh->GetFd(), LockDataType::TABLE};
        //     context->txn_->GetLockSet()->insert(lock_data_id);
        // }
        if (index_no != -1) {
            // printf("index_no: %d\n", index_no);
            table_scan_executors[i] =
                std::make_unique<IndexScanExecutor>(sm_manager_, tab_names[i], curr_conds, index_no, context);
        } else {
            // printf("no index\n");
            table_scan_executors[i] = std::make_unique<SeqScanExecutor>(sm_manager_, tab_names[i], curr_conds, context);
        }
    }
    assert(conds.empty());

    std::unique_ptr<AbstractExecutor> executorTreeRoot = std::move(table_scan_executors.back());

    // lab3 task2 Todo
    // 构建算子二叉树
    // 逆序遍历tab_nodes为左节点, 现query_plan为右节点,生成joinNode作为新query_plan 根节点
    // 生成query_plan tree完毕后, 根节点转换成投影算子
    // lab3 task2 Todo End
    for (int i = tab_names.size() - 2; i >= 0; i--) {
        std::unique_ptr<AbstractExecutor> left = std::move(table_scan_executors[i]);
        std::unique_ptr<AbstractExecutor> right = std::move(executorTreeRoot);
        executorTreeRoot = std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right));
    }
    executorTreeRoot = std::make_unique<ProjectionExecutor>(std::move(executorTreeRoot), sel_cols);
    // 创建了select+order的投影后，注意一下这里的抬头输出，所以还是先删掉
    sel_cols.erase(sel_cols.end()-order_cols.size(),sel_cols.end());
    // Column titles
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col : sel_cols) {
        captions.push_back(sel_col.col_name);
    }
    // Print header
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // Print records
    size_t num_rec = 0;
    std::vector<OrderUnit> all_table;//所有的select+order列结果
    // 执行query_plan
    for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
        auto Tuple = executorTreeRoot->Next();
        std::vector<std::string> columns;
        for (auto &col : executorTreeRoot->cols()) {
            std::string col_str;
            char *rec_buf = Tuple->data + col.offset;
            if (col.type == TYPE_INT) {
                col_str = std::to_string(*(int *)rec_buf);
            } else if (col.type == TYPE_FLOAT) {
                col_str = std::to_string(*(float *)rec_buf);
            } else if (col.type == TYPE_STRING) {
                col_str = std::string((char *)rec_buf, col.len);
                col_str.resize(strlen(col_str.c_str()));
            }
            columns.push_back(col_str);
            if(coltype.size()<is_asc.size())coltype.push_back(col.type);
        }
        // rec_printer.print_record(columns, context);
        OrderUnit tmp;
        tmp.tuple=columns;
        tmp.is_asc=is_asc;
        tmp.coltype=coltype;
        all_table.push_back(tmp);
        // num_rec++;
        // if(limit>0&&num_rec==limit)
        // {
        //     break;
        // }
    }
    // TODO 现在all_table里面有select+order by的列的所有结果，需要排序了
    // TODO 这里可以用vector<string>的swap直接交换两列，所以需要自己写一个快排，输入两个vector<string>，要比较的列号，排序是升序还是降序
    // !注意此时的sel_col已经恢复正常，只是all_table里面有所有的结果
    // !排序完后记得砍掉所有order _by列(其实就是把vector<string>的最后几个忽略掉)
    std::sort(all_table.begin(),all_table.end());
    for(std::size_t i=0;i<all_table.size();i++)
    {
        std::vector<std::string> columns;
        columns.assign(all_table[i].tuple.begin(),all_table[i].tuple.begin()+sel_cols.size());
        rec_printer.print_record(columns, context);
        num_rec++;
        if(limit>0&&num_rec==(size_t)limit)
        {
            break;
        }
    }



    // Print footer
    rec_printer.print_separator(context);
    // Print record count
    RecordPrinter::print_record_count(num_rec, context);

}