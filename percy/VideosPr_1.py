# encoding: utf-8
'''
Created on 2016年6月6日

feature:
1,建立各个视频间的先后概率关系
note:
1,看视频定义:观看内容在1/5之上，否视为无效数据
2,连续观看两个视频定义：两周之内
@author: wangguojie
'''

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.abspath(__file__),
                                             os.pardir, os.pardir, os.pardir)))
from util.setting import *

processes = multiprocessing.cpu_count() - 1
if len(sys.argv) > 1:
    processes = int(sys.argv[1])
print("processes=" + str(processes))

video_others = {}

# data statistics analysis,generate required data
def processing():
    load_video_attr()
    user_list = get_user_list()
#     user_list = user_list[0:10000]
    pool = multiprocessing.Pool(processes=processes)
    cnt = 0
    result_list = []

    # test
#     result = sub_processing(user_list[0:1000])
#     print result
#     sys.exit(0)

    while(cnt < len(user_list)):
        result_list.append(pool.apply_async(sub_processing, (user_list[cnt:cnt + 5000],)))
        cnt += 5000
    pool.close()
    pool.join()
    
    
    global video_others
    for result in result_list:
#         try:
        sub_video_others = result.get(timeout = 10)
#         except:
#             print sub_video_others
#             print 'ggggg', sys.exc_info()
#             sys.exit(0)
        
#         print 'sub_video_others',sub_video_others
        
        for video in sub_video_others:
            if video in video_others:
                video_others[video] += sub_video_others[video]
            else:
                video_others.update({video:sub_video_others[video]})    
        
def get_user_list():
    user_list = []
    users_file = os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir, os.pardir, "data","users.txt")) 
    with open(users_file) as o:
        for data in o:
            user_list.append(data.rstrip('\n'))
    return user_list
#     redis_start("redis_answers")
#     user_list = redis_ability.keys()
#     redis_stop("redis_answers")
#     return user_list if len(user_list) > 0 else []

video_duration_dict = {}
def load_video_attr():
    base_data_dir = os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir, os.pardir, os.pardir , "base", "data"))    
#     print os.path.join(base_data_dir, "video_attr")
    video_attr_db = shelve.open(os.path.join(base_data_dir, "video_attr"))
#     print len(video_attr_db.keys())
    global video_duration_dict
    video_duration_dict = dict(video_attr_db)
#     print len(video_duration_dict)
    video_attr_db.close()
    
from bson.objectid import ObjectId 
# sub processing,when multiprocessing
def sub_processing(user_list):
    print(str(datetime.datetime.now()) + "\tPID = " + str(multiprocessing.current_process().pid) + " start...")

    sub_video_others = {} # {video_id1:{'video_id2':cnt,......}}  
    events_koala = MongoClient(host='10.8.8.111', port=27017,connect=False)["koalaBackupOnline"]["events"]
    cursor  = events_koala.find({"user":{"$in":map(ObjectId,user_list)},"category":"video","publisher":"人教版"} , {"eventKey":1,"eventValue":1,"eventTime":1,"user":1,"_id" : 0}).sort("user")
    user = None
    user_records = []
    for record in cursor:
        #print record
        try:
            if record['user'] == user or user is None:
                if user is None:
                    user = record['user']
                user_records.append((round(record['eventTime']/1000, 3), str(record['eventKey']), record['eventValue']))
            else:
                #print user_records
                user_video_seqs = video_time_seq(user_records)
                for user_video_seq in user_video_seqs:
                    for i in range(len(user_video_seq) - 1):
                        video_this = user_video_seq[i][0]
                        video_next = user_video_seq[i + 1][0]
                        if video_this in sub_video_others:
                            sub_video_others[video_this].update({video_next})
                        else:
                            sub_video_others.update({video_this:collections.Counter({video_next})})
                         
                user_records = [(round(record['eventTime']/1000, 3), str(record['eventKey']), record['eventValue'])]
                user = record['user']
        except:
            user = None
            user_records = []
#             print '1111',sys.exc_info()
#             raise
            
    try:
        if len(user_records) > 0:
                user_video_seqs = video_time_seq(user_records)
                for user_video_seq in user_video_seqs:
                    for i in range(len(user_video_seq) - 1):
                        video_this = user_video_seq[i][0]
                        video_next = user_video_seq[i + 1][0]
                        if video_this in sub_video_others:
                            sub_video_others[video_this].update({video_next})
                        else:
                            sub_video_others.update({video_this:collections.Counter({video_next})})
    except:
        pass
#         print '2222',sys.exc_info() 
#         raise
       
    print(str(datetime.datetime.now()) + "\tPID = " + str(multiprocessing.current_process().pid) + " finished.")
    print len(sub_video_others)
      
    return sub_video_others


# 针对某个学生的观看视频记录
# 返回该学生观看视频的序列及对应视频的时间 [[(video_1,time),(video_2,time)......],,,]
# 返回的是多层用户观看视频的序列
# 观看时长小于1/5视频时长视为无效,时长大于5倍的视为5倍
def video_time_seq(records):
    time_seq = []
    records.sort(key=lambda tup:tup[0])

    video_id = None
    atom_records = []
    for record in records:
        #print record
        try:
            if record[2]['videoId'] == video_id or video_id is None:
                if video_id is None:
                    video_id = record[2]['videoId']
                atom_records.append(record)
            else:
                avts = atom_video_time_seq(atom_records)
                if len(avts) > 0:
                    time_seq.append(avts)
                    
                atom_records = [record]
                video_id = record[2]['videoId']
        except:
            video_id = None
            atom_records = []
            #print record
            #raise
    try:
        if len(atom_records) > 0:
            avts = atom_video_time_seq(atom_records)
            if len(avts) > 0:
                time_seq.append(avts)
    except:
        pass
        #for test
#         print '3333',sys.exc_info()
#         raise
    
    
#     print time_seq


    #处理time_seq连续重复的视频及时间不满足预设的两周以内的
    for i in range(len(time_seq) - 1):
        if i < len(time_seq) - 1:
            if time_seq[i][0] == time_seq[i + 1][0]:
                time_seq[i][1] = stdTime(time_seq[i][1] ,time_seq[i + 1][1])
                time_seq.pop(i)
        else:
            break
    
    # in two weeks
    total_time_seq = []
    start = 0
    for i in range(len(time_seq) - 1):
        if abs(time_seq[i + 1][2] - time_seq[i][2]) > 14*24*60*60:
            if i + 1 - start > 1:
                total_time_seq.append(time_seq[start : i + 1])
            start = i + 1
    if len(time_seq) - start > 1:
        total_time_seq.append(time_seq[start :])
    
    return total_time_seq


def stdTime(time_1, time_2):
    return time_1 + time_2 if time_1 + time_2 <= 5 else 5
        
# 针对某个学生观看某个视频的记录
# 返回videoID及观看该视频时间
# records 已经按照时间排序的数据
def atom_video_time_seq(records):
    video_id = str(records[0][2]['videoId'])
    global video_duration_dict
    
    try:
        video_duration = video_duration_dict[video_id]['duration']
    except:
        return []
    start_event_time = records[0][0]
    start_video_time = 0 #  video time
    event_forward = records[0][1]
    watch_video_time = 0
    watch_time = 0
    try:
        for record in records[1:]:
            if record[1] == "answerVideoInteraction":
                start_event_time += Config.config.getfloat('para', 'VIDEO_AVER_ANS_INTERACTION_TIME')
                event_forward = record[1]
                continue
            
            if event_forward == "pauseVideo":
                delta_event_time = record[0] - start_event_time
                watch_time += delta_event_time if delta_event_time <= video_duration else video_duration
            
            if record[1] == "pauseVideo" or record[1] == "quitVideo" or record[1] == "finishVideo"  \
                            or record[1] == "dragVideo" :
                if record[1] == "pauseVideo":
                    cur_video_time = round(record[2]['pauseTime']/1000, 3)
                elif record[1] == "quitVideo":
                    cur_video_time = round(record[2]['quitTimeStamp']/1000, 3)
                elif record[1] == "finishVideo":
                    cur_video_time = video_duration
                elif record[1] == "dragVideo":
                    cur_video_time = round(record[2]['fromTimeStamp']/1000, 3)
    
                    #print 'start video time',start_video_time,'cur video time',cur_video_time
    
                delta_video_time = cur_video_time - start_video_time
                delta_event_time = record[0] - start_event_time
                if delta_video_time > delta_event_time:
                    start_video_time = cur_video_time - delta_event_time
                if event_forward != "pauseVideo":
                    watch_video_time += cur_video_time - start_video_time
                start_video_time = cur_video_time
                if record[1] == "dragVideo":
                    start_video_time = round(record[2]['toTimeStamp']/1000, 3)  
                    
                watch_time += delta_event_time
                
            else:
                if event_forward != "pauseVideo":
                    delta_event_time = record[0] - start_event_time
                    start_video_time = start_video_time + record[0] - start_event_time
                    watch_video_time += delta_event_time
                    
                    watch_time += delta_event_time
                    
            start_event_time = record[0]
            event_forward = record[1]
    except:
        pass
#         print 'atom_video_time_seq',sys.exc_info()
        
    if watch_video_time < Config.config.getfloat('para','WATCH_VIDEO_TIME_MIN')*video_duration:
        return []
    
    watch_time = watch_time*1.0/video_duration
    watch_time = 5 if watch_time > 5 else watch_time
    return [video_id,round(watch_time,3),start_event_time]
    
# out data to out file
def out():
    out_file = os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir, os.pardir, "data","videos_pr"))    
    videos_pr_db = shelve.open(out_file)
    global video_others
    for video in video_others:
        videos_pr_db[video] = video_others[video]
    videos_pr_db.close()

if __name__ == '__main__':
    try:
        print("start data statistics and analysis ......")
        processing()
        print("data processing finished.")
        
        print("saving result......")
        out()
        print("task over.Bye!")
    except:
        print("data processing error...... -->" + __file__)
        # test over, should log and no raise
        raise
