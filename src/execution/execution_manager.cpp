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

#define DEBUG 0

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
    auto Insert = std::make_unique<InsertExecutor>(sm_manager_, tab_name, values, context);
    Insert->Next();
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
    int index_no = get_indexNo(tab_name, conds);
    if(index_no != -1) {
        scanExecutor =  std::make_unique<IndexScanExecutor>(sm_manager_, tab_name, conds, index_no, context);
    }
    else {
        scanExecutor = std::make_unique<SeqScanExecutor>(sm_manager_, tab_name, conds, context);
    }

    for (scanExecutor->beginTuple(); !scanExecutor->is_end(); scanExecutor->nextTuple()) {
        rids.push_back(scanExecutor->rid());
    }

    // lab3 task3 Todo
    // make deleteExecutor
    // call deleteExecutor.Next()
    // lab3 task3 Todo end
    auto Delete = std::make_unique<DeleteExecutor>(sm_manager_, tab_name, conds, rids, context);
    Delete->Next();
}

void QlManager::update_set(const std::string &tab_name, std::vector<SetClause> set_clauses,
                           std::vector<Condition> conds, Context *context) {
    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    // Parse where clause
    conds = check_where_clause({tab_name}, conds);
    // Get raw values in set clause
    for (auto &set_clause : set_clauses) {
        if(DEBUG)std::cout<<set_clause.lhs.col_name<<std::endl;
        auto lhs_col = tab.get_col(set_clause.lhs.col_name);
        if (lhs_col->type != set_clause.rhs.type) {
            throw IncompatibleTypeError(coltype2str(lhs_col->type), coltype2str(set_clause.rhs.type));
        }
        set_clause.rhs.init_raw(lhs_col->len);
        if(DEBUG)std::cout<<set_clause.rhs.int_val<<' '<<set_clause.rhs.float_val<<' '<<set_clause.rhs.str_val<<std::endl;
    }
    // Get all RID to update
    std::vector<Rid> rids;

    // lab3 task3 Todo
    // make scan executor
    std::unique_ptr<AbstractExecutor> scanExecutor;
    int index_no = get_indexNo(tab_name, conds);
    if(index_no != -1) {
        scanExecutor =  std::make_unique<IndexScanExecutor>(sm_manager_, tab_name, conds, index_no, context);
    }
    else {
        scanExecutor = std::make_unique<SeqScanExecutor>(sm_manager_, tab_name, conds, context);
    }

    for (scanExecutor->beginTuple(); !scanExecutor->is_end(); scanExecutor->nextTuple()) {
/*      auto Tuple = scanExecutor->Next();
        for (auto &set_clause : set_clauses) {
            auto lhs_col = tab.get_col(set_clause.lhs.col_name);
            memcpy(Tuple->data + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len);
        } */
        rids.push_back(scanExecutor->rid());
    }
    // 将rid存入rids
    // make updateExecutor
    // call updateExecutor.Next()
    // lab3 task3 Todo end
    auto Update = std::make_unique<UpdateExecutor>(sm_manager_, tab_name, set_clauses, conds, rids, context);
    Update->Next();
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
 * @param orders select plan 排序条件
 */
void QlManager::select_from(std::vector<TabCol> sel_cols, const std::vector<std::string> &tab_names,
                            std::vector<Condition> conds, std::vector<Ordercon> orders, int limit_num, Context *context) {
    // Parse selector
    if(DEBUG) std::cout<<"start from select"<<std::endl;
    auto all_cols = get_all_cols(tab_names);
    if(DEBUG) std::cout<<"is no selection:"<< sel_cols.empty() <<std::endl;
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
        if(DEBUG) std::cout<<"index_no="<<index_no<<std::endl;
        if(index_no != -1) {
            table_scan_executors[i] = 
                std::make_unique<IndexScanExecutor>(sm_manager_, tab_names[i], curr_conds, index_no, context);
        }
        else {
            table_scan_executors[i] =
                std::make_unique<SeqScanExecutor>(sm_manager_, tab_names[i], curr_conds, context);
            if(DEBUG) std::cout<<"finish insert"<<std::endl;
        }
    }
    assert(conds.empty());
    int tab_names_len = tab_names.size();
    std::unique_ptr<AbstractExecutor> executorTreeRoot = std::move(table_scan_executors[tab_names_len-1]);

    // lab3 task2 Todo
    // 构建算子二叉树
    // 逆序遍历tab_nodes为左节点, 现query_plan为右节点,生成joinNode作为新query_plan 根节点
    // 生成query_plan tree完毕后, 根节点转换成投影算子
    // lab3 task2 Todo End
    if(DEBUG) std::cout<< "start proj" <<std::endl;
    if(tab_names_len > 1) {
        for(int i = tab_names_len - 2; i>=0 ; i--) {
            executorTreeRoot = std::make_unique<NestedLoopJoinExecutor>(std::move(table_scan_executors[i]), std::move(executorTreeRoot));
        }
    }
    executorTreeRoot = std::make_unique<ProjectionExecutor>(std::move(executorTreeRoot), sel_cols);
    if(DEBUG) std::cout<< "end proj" <<std::endl;
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
    // Print order
    std::vector<int> sv2tab;
    std::vector<int> svorder;
    for(auto &sv_order: orders) {
        if(DEBUG)std::cout << sv_order.col_name << ' ' << sv_order.order_name << std::endl;
        svorder.push_back((sv_order.order_name.compare("ASC") == 0));
        for(int i=0;i<captions.size();++i) {
            auto tab_col_name = captions[i];
            if(tab_col_name.compare(sv_order.col_name) == 0) {sv2tab.push_back(i); break;}
        }
    }
    for(int i=0;i<orders.size();++i)
        if(DEBUG)std::cout<< sv2tab[i] << ' ' << svorder[i]<< std::endl;
    // Print records
    size_t num_rec = 0;
    // 执行query_plan
    std::vector<std::vector<std::string>> ans;
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
        ans.push_back(columns);
        num_rec++;
    }
    for(int k=orders.size()-1;k>=0;--k) 
        for(int i=0;i<ans.size();++i)
            for(int j=i+1;j<ans.size();++j) {
                int num = sv2tab[k];
                if(DEBUG)std::cout<< svorder[k] << ' '<< ans[i][num] << ' '<<ans[j][num]<<std::endl;
                if(ans[i][num] == ans[j][num]) continue;
                int com_result = ans[i][num] < ans[j][num];
                if(ans[i][num][0]=='-' && ans[j][num][0]=='-') com_result^=1;
                int need_result = svorder[k];
                if(com_result ^ need_result) swap(ans[i],ans[j]);
            }
    int out_len = ans.size();
    if(limit_num!=-1) out_len = limit_num;
    for(int i=0;i<out_len;++i) rec_printer.print_record(ans[i], context);
    ans.clear();
    // Print footer
    rec_printer.print_separator(context);
    // Print record count
    RecordPrinter::print_record_count(out_len, context);
}
