#include "replacer/clock_replacer.h"

#include <algorithm>

ClockReplacer::ClockReplacer(size_t num_pages)
    : circular_{num_pages, ClockReplacer::Status::EMPTY_OR_PINNED}, hand_{0}, capacity_{num_pages} {
    // 成员初始化列表语法
    circular_.reserve(num_pages);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    // Todo: try to find a victim frame in buffer pool with clock scheme
    // and make the *frame_id = victim_frame_id
    // not found, frame_id=nullptr and return false
    frame_id_t fuben=hand_;
    bool flag=false;
    while(true)
    {
        hand_++;
        hand_=hand_%ClockReplacer::capacity_;
        if(fuben==hand_&&!flag)
        {
            frame_id=nullptr;
            return false;
        }
        if(circular_[hand_]==Status::UNTOUCHED)
        {
            flag=true;
            *frame_id=hand_;
            circular_[hand_]=Status::EMPTY_OR_PINNED;
            return true;
        }
        else if(circular_[hand_]==Status::ACCESSED)
        {
            flag=true;
            circular_[hand_]=Status::UNTOUCHED;
        }
    }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    // Todo: you can implement it!
    circular_[frame_id]=Status::EMPTY_OR_PINNED;
    
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    const std::lock_guard<mutex_t> guard(mutex_);
    // Todo: you can implement it!
    if(circular_[frame_id]==Status::EMPTY_OR_PINNED)
    {
        circular_[frame_id]=Status::ACCESSED;
    }
}

size_t ClockReplacer::Size() {
    const std::lock_guard<mutex_t> guard(mutex_);

    // Todo:
    // 返回在[arg0, arg1)范围内满足特定条件(arg2)的元素的数目
    // return all items that in the range[circular_.begin, circular_.end )
    // and be met the condition: status!=EMPTY_OR_PINNED
    // That is the number of frames in the buffer pool that storage page (NOT EMPTY_OR_PINNED)
    size_t num=0;
    for(size_t i=0;i<ClockReplacer::capacity_;i++)
    {
        if(circular_[i]!=Status::EMPTY_OR_PINNED)
        {
            
            num = num+1;
        }
    }
    return num;
}
