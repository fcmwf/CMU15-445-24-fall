//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Spring 2025: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join
 * @param[out] rid The next tuple RID produced, not used by nested loop join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
	RID left_rid;
	while (right_idx >= 0 || left_executor_->Next(&left_tuple_, &left_rid)) {
	// std::cout << "left: " << left_tuple_.ToString(&left_executor_->GetOutputSchema()) << std::endl;
		right_executor_->Init();
		Tuple t;
		RID rrid;
		std::vector<Tuple> right_tuple_;
		while (right_executor_->Next(&t, &rrid)) {
		// std::cout << "right: " << t.ToString(&right_executor_->GetOutputSchema()) << std::endl;
			right_tuple_.push_back(t);
		}
		for (int rdx = right_idx == -1 ? 0 : right_idx; rdx < int(right_tuple_.size()); rdx++) {

			auto value = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
													&right_tuple_[rdx], right_executor_->GetOutputSchema());
			if (!value.IsNull() && value.GetAs<bool>()) {

				std::vector<Value> values;
				for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
					values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
				}
				for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
					values.push_back(right_tuple_[rdx].GetValue(&right_executor_->GetOutputSchema(), i));
				}
				//std::cout << "index: "<< rdx << std::endl;
				//std::cout << right_tuple_[rdx].GetValue(&right_executor_->GetOutputSchema(), 0).ToString() << std::endl;
				*tuple = Tuple{values, &GetOutputSchema()};
				// std::cout << tuple->ToString(&GetOutputSchema()) << std::endl;
				right_idx = rdx + 1;
				return true;
			}
		}

		if (right_idx == -1 && plan_->GetJoinType() == JoinType::LEFT) {
			std::vector<Value> values;
			for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
				values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
			}
			for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
				values.push_back(
					ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
			}

			*tuple = Tuple{values, &GetOutputSchema()};
			return true;
		}
		right_idx = -1;
	}

	return false;
}

}  // namespace bustub
