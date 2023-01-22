#include "lock_manager.h"

#define group_mode lock_table_[lock_id].group_lock_mode_
#define group_mode_read (group_mode != GroupLockMode::S && group_mode != GroupLockMode::IS &&  group_mode != GroupLockMode::NON_LOCK)
#define group_mode_write (group_mode != GroupLockMode::NON_LOCK)

/*
    IS  S  U  IX  SIX  X  (now mode)
IS  Y   Y  Y  Y   Y    N
S   Y   Y  Y  N   N    N
U   Y   Y  N  N   N    N
IX  Y   N  N  Y   N    N
SIX Y   N  N  N   N    N
X   N   N  N  N   N    N
(requset_mode)
*/



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
    std::unique_lock<std::mutex> lock{latch_};
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) return false;
    if (txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操
    /* 
    LockRequestQueue *lock_queue = &lock_table_[LockDataId(tab_fd, LockDataType::TABLE)];
    auto iter = lock_queue->request_queue_.begin();
    for (; iter != lock_queue->request_queue_.end(); ++iter) 
        if (iter->txn_id_ == txn->GetTransactionId()) break;
    */
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    txn->GetLockSet()->insert(lock_id);
    LockRequest *need_lock = new LockRequest(txn->GetTransactionId(), LockMode::SHARED);
    lock_table_[lock_id].shared_lock_num_++;
    lock_table_[lock_id].request_queue_.push_back(*need_lock);
    // group mode
    while (group_mode_read)
        lock_table_[lock_id].cv_.wait(lock);
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    need_lock->granted_ = true;
    lock_table_[lock_id].cv_.notify_all();
    lock.unlock();
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示
    return true;
}

/**
 * 申请行级写锁
 * @param txn 要申请锁的事务对象指针
 * @param rid 加锁的目标记录ID
 * @param tab_fd 记录所在的表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockExclusiveOnRecord(Transaction *txn, const Rid &rid, int tab_fd) {
    // Todo:
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock{latch_};
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) return false;
    if (txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操
    /* 
    LockRequestQueue *lock_queue = &lock_table_[LockDataId(tab_fd, LockDataType::TABLE)];
    auto iter = lock_queue->request_queue_.begin();
    for (; iter != lock_queue->request_queue_.end(); ++iter) 
        if (iter->txn_id_ == txn->GetTransactionId()) break;
    */
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    txn->GetLockSet()->insert(lock_id);
    LockRequest *need_lock = new LockRequest(txn->GetTransactionId(), LockMode::SHARED);
    lock_table_[lock_id].shared_lock_num_++;
    lock_table_[lock_id].request_queue_.push_back(*need_lock);
    // group mode
    while (group_mode_write)
        lock_table_[lock_id].cv_.wait(lock);
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    lock_table_[lock_id].group_lock_mode_ = GroupLockMode::X;
    need_lock->granted_ = true;
    lock_table_[lock_id].cv_.notify_all();
    lock.unlock();
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示
    return true;
}

/**
 * 申请表级读锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockSharedOnTable(Transaction *txn, int tab_fd) {
    // Todo:
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock{latch_};
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) return false;
    if (txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    /* 
    LockRequestQueue *lock_queue = &lock_table_[LockDataId(tab_fd, LockDataType::TABLE)];
    auto iter = lock_queue->request_queue_.begin();
    for (; iter != lock_queue->request_queue_.end(); ++iter) 
        if (iter->txn_id_ == txn->GetTransactionId()) break;
    */
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    txn->GetLockSet()->insert(lock_id);
    LockRequest *need_lock = new LockRequest(txn->GetTransactionId(), LockMode::SHARED);
    lock_table_[lock_id].shared_lock_num_++;
    lock_table_[lock_id].request_queue_.push_back(*need_lock);
    // group mode
    while (group_mode_read)
        lock_table_[lock_id].cv_.wait(lock);
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    if (group_mode == GroupLockMode::IX)
        lock_table_[lock_id].group_lock_mode_ = GroupLockMode::SIX;
    else if (group_mode == GroupLockMode::IS || group_mode == GroupLockMode::NON_LOCK)
        lock_table_[lock_id].group_lock_mode_ = GroupLockMode::S;
    need_lock->granted_ = true;
    lock_table_[lock_id].cv_.notify_all();
    lock.unlock();
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示
    return true;
}

/**
 * 申请表级写锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockExclusiveOnTable(Transaction *txn, int tab_fd) {
    // Todo:
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock{latch_};
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) return false;
    if (txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    /* 
    LockRequestQueue *lock_queue = &lock_table_[LockDataId(tab_fd, LockDataType::TABLE)];
    auto iter = lock_queue->request_queue_.begin();
    for (; iter != lock_queue->request_queue_.end(); ++iter) 
        if (iter->txn_id_ == txn->GetTransactionId()) break;
    */
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    txn->GetLockSet()->insert(lock_id);
    LockRequest *need_lock = new LockRequest(txn->GetTransactionId(), LockMode::SHARED);
    lock_table_[lock_id].shared_lock_num_++;
    lock_table_[lock_id].request_queue_.push_back(*need_lock);
    // group mode
    while (group_mode_write)
        lock_table_[lock_id].cv_.wait(lock);
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    lock_table_[lock_id].group_lock_mode_ = GroupLockMode::X;
    need_lock->granted_ = true;
    lock_table_[lock_id].cv_.notify_all();
    lock.unlock();
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示
    return true;
}

/**
 * 申请表级意向读锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockISOnTable(Transaction *txn, int tab_fd) {
    // Todo:
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock{latch_};
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) return false;
    if (txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    /* 
    LockRequestQueue *lock_queue = &lock_table_[LockDataId(tab_fd, LockDataType::TABLE)];
    auto iter = lock_queue->request_queue_.begin();
    for (; iter != lock_queue->request_queue_.end(); ++iter) 
        if (iter->txn_id_ == txn->GetTransactionId()) break;
    */
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    txn->GetLockSet()->insert(lock_id);
    LockRequest *need_lock = new LockRequest(txn->GetTransactionId(), LockMode::SHARED);
    lock_table_[lock_id].shared_lock_num_++;
    lock_table_[lock_id].request_queue_.push_back(*need_lock);
    // group mode
    while (group_mode == GroupLockMode::X)
        lock_table_[lock_id].cv_.wait(lock);
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    if (group_mode == GroupLockMode::NON_LOCK)
        lock_table_[lock_id].group_lock_mode_ = GroupLockMode::IS;
    need_lock->granted_ = true;
    lock_table_[lock_id].cv_.notify_all();
    lock.unlock();
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示
    return true;
}

/**
 * 申请表级意向写锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockIXOnTable(Transaction *txn, int tab_fd) {
    // Todo:
    // 1. 通过mutex申请访问全局锁表
    std::unique_lock<std::mutex> lock{latch_};
    // 2. 检查事务的状态
    if (txn->GetState() == TransactionState::ABORTED) return false;
    if (txn->GetState() == TransactionState::SHRINKING){
        txn->SetState(TransactionState::ABORTED);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操
    /* 
    LockRequestQueue *lock_queue = &lock_table_[LockDataId(tab_fd, LockDataType::TABLE)];
    auto iter = lock_queue->request_queue_.begin();
    for (; iter != lock_queue->request_queue_.end(); ++iter) 
        if (iter->txn_id_ == txn->GetTransactionId()) break;
    */
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    txn->GetLockSet()->insert(lock_id);
    LockRequest *need_lock = new LockRequest(txn->GetTransactionId(), LockMode::SHARED);
    lock_table_[lock_id].shared_lock_num_++;
    lock_table_[lock_id].request_queue_.push_back(*need_lock);
    // group mode    
    while (group_mode == GroupLockMode::S && group_mode == GroupLockMode::X && group_mode == GroupLockMode::SIX)
        lock_table_[lock_id].cv_.wait(lock);
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    if (group_mode == GroupLockMode::IS || group_mode == GroupLockMode::NON_LOCK)
        lock_table_[lock_id].group_lock_mode_ = GroupLockMode::IX;
    else if (group_mode == GroupLockMode::S)
        lock_table_[lock_id].group_lock_mode_ = GroupLockMode::SIX;
    need_lock->granted_ = true;
    lock_table_[lock_id].cv_.notify_all();
    lock.unlock();
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示
    return true;
}

/**
 * 释放锁
 * @param txn 要释放锁的事务对象指针
 * @param lock_data_id 要释放的锁ID
 * @return 返回解锁是否成功
 */
bool LockManager::Unlock(Transaction *txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);
    txn->SetState(TransactionState::SHRINKING);
    // 未申请
    if (txn->GetLockSet()->find(lock_data_id) == txn->GetLockSet()->end()) return false;
    // 查找当前事务
    auto it = lock_table_[lock_data_id].request_queue_.begin();
    for (;it != lock_table_[lock_data_id].request_queue_.end(); it++) {
        if (it->txn_id_ == txn->GetTransactionId()) break;
    }
    GroupLockMode temp_mode = GroupLockMode::NON_LOCK;
    it = lock_table_[lock_data_id].request_queue_.begin();
    // update group_mode
    for (;it != lock_table_[lock_data_id].request_queue_.end(); it++) {
        if (it->granted_ == true) {
            switch(it->lock_mode_)
            {
            case LockMode::SHARED:
                if(temp_mode == GroupLockMode::SIX) continue;
                temp_mode = temp_mode == GroupLockMode:: IX ? GroupLockMode::SIX : GroupLockMode::S;
                break;
            case LockMode::EXLUCSIVE:
                temp_mode = GroupLockMode::X;
                break;
            case LockMode::INTENTION_SHARED:
                if(temp_mode == GroupLockMode::NON_LOCK || temp_mode == GroupLockMode::IS)
                    temp_mode = GroupLockMode::IS;
                break;
            case LockMode::INTENTION_EXCLUSIVE:
                if(temp_mode == GroupLockMode::SIX) continue;
                temp_mode = temp_mode == GroupLockMode:: S ? GroupLockMode::SIX : GroupLockMode::IX;
                break;
            case LockMode::S_IX:
                temp_mode = GroupLockMode::SIX;
                break;
            }
        }
    }
    lock_table_[lock_data_id].group_lock_mode_ = temp_mode;
    lock_table_[lock_data_id].cv_.notify_all();
    lock.unlock();
    return true;
}