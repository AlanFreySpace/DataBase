//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {
/**
   * 创建SeqScanExecutor实例.
   * @param exec_ctx 执行器的上下文
   * @param plan 要执行的sequential scan plan
   */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
    : AbstractExecutor(exec_ctx), plan_(plan), table_heap_(nullptr), iter_(nullptr, RID(), nullptr) {}

// 利用上下文context以及plan得到table 得到对table遍历器的起点iter_
void SeqScanExecutor::Init() {
    table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
    iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

// 筛选被返回的列Select...以及筛选要返回的行from...where...
bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // 遍历完了返回false
  if (iter_ == table_heap_->End()) {
    return false;
  }

  // 获取RID和要返回的列
  RID original_rid = iter_->GetRid();
  const Schema *output_schema = plan_->OutputSchema();

  // 筛选要返回的列的值
  std::vector<Value> vals;
  vals.reserve(output_schema->GetColumnCount());
  for (size_t i = 0; i < vals.capacity(); i++) {
    vals.push_back(output_schema->GetColumn(i).GetExpr()->Evaluate(
        &(*iter_), &(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)));
  }

  // 迭代器+1
  ++iter_;

  // 构造要返回的行
  Tuple temp_tuple(vals, output_schema);

  // 利用谓词判断该行符不符合条件，符合则返回，不符合就继续找下一行
  const AbstractExpression *predict = plan_->GetPredicate();
  if (predict == nullptr || predict->Evaluate(&temp_tuple, output_schema).GetAs<bool>()) {
    *tuple = temp_tuple;
    *rid = original_rid;
    return true;
  }
  return Next(tuple, rid);
}
}  // namespace bustub
