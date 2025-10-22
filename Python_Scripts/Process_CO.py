import os
import stat
import pathlib
import inspect
import multiprocessing
import glob
import pandas as pd
import datetime
import xlrd
import calendar
import numpy as np
import time

def read_conf_file(fname):
    df = pd.read_csv(fname,delimiter = "\t")
    date_cols = [i for i in df.columns.values if "date" in i.lower()]
    for i in date_cols:
        df[i] = pd.to_datetime(df[i])
    return df

def read_conf_file_list(fname,splitter = None,item_number = None):
    with open(fname) as infile:
        return_list = [i.strip() for i in infile.readlines()]
    if splitter !=None:
        return_list = [i.split(splitter)[item_number] for i in return_list]
    return return_list

def filter_flist(flist,bad_files,year):
    flist = list(set(flist) - set(bad_files))
    start_date = int(str(year-1) + "1229")
    #start_date = int(((datetime.datetime.now() - datetime.timedelta(days=32)).replace(day=1) - datetime.timedelta(days = 2)).strftime("%Y%m%d"))
    #end_date = int(datetime.datetime.now().strftime("%Y%m%d"))
    end_date = int(str(year+1) + "0102")
    flist = [i for i in flist if ((gen_date(i) >= start_date) and (gen_date(i) <= end_date))]
    return flist

def gen_date(i):
    try:
        return int(os.path.basename(i).split("-")[1])
    except:
        return int("30000101")

def get_df_list(flist):
    df_list = []
    multi = True
    if multi:
        mypool = multiprocessing.Pool(10)
        result = mypool.map_async(read_file,flist,chunksize = 1)   
        mypool.close()
        previous_number = result._number_left
        while True:
            if (result.ready()): break
            remaining = result._number_left
            if remaining != previous_number:
                print ("Waiting for", remaining, "files to be read...")
                previous_number = remaining
            time.sleep(5)
        df_list = result.get()       
    else:       
        for i in flist:
            df_list.append(read_file(i))
            
    return df_list
        
def read_file(fname):
    spec_dict = {4:["CO"],2:["CH4_dry","CH4"],3:["H2O"],1:["CO2_dry","CO2"]}
    spec_list = ["CO","CH4_dry","CH4","H2O","CO2_dry","CO2","solenoid_valves","species"]
    df_list = []
    try:
        df = pd.read_csv(fname,delim_whitespace = True,parse_dates =  {"DateTime" : [0,1]},date_parser = get_date)
        df = df.loc[df["ALARM_STATUS"] != 1].set_index("DateTime")
        df["CO"] = df["CO"]*1000
        df = df.drop([i for i in df.columns.values if i not in spec_list],axis = 1)
        for valve,species in spec_dict.items():
            df_list.append(df.loc[df["species"] == valve,species + ["solenoid_valves"]])
    except Exception as e:
        df_list = str(fname) + "\t" + str(e)
    return df_list

def get_date(day_val,time_val):
    return(np.datetime64(day_val + "T" + time_val))

def split_df_list(df_list):
    successes = [i for i in df_list if isinstance(i,list)]
    fails = [i for i in df_list if not isinstance(i,list)]
    return (fails,successes)

def drop_dates(df,date_list):
    for i in date_list.to_dict("records"):
        try:
            df = df.drop(df[i["Start Date"]:i["End Date"]].index.values)
            print("Dropped",i["Start Date"],i["End Date"])
        except:
            pass
    return df

def output_df(df,output_dir,year):
    create_folder([output_dir,str(year)])
    print("trimming year")
    df = df.loc[df.index.year == year]
    print("getting unique month")
    for x in pd.unique(df.index.month):
        print("creating folder")
        create_folder([output_dir,str(year),calendar.month_name[x]])
        
        output_month(df,output_dir,str(year),x)

def create_folder(path_parts):
    os.makedirs(os.path.join(*list(map(str,path_parts))),exist_ok = True)

def output_month(df,output_dir,year,month):
    print("outputting month")
    df.loc[((df.index.year == int(year)) & (df.index.month == int(month)))].to_csv(os.path.join(*list(map(str,[output_dir,year,calendar.month_name[month],calendar.month_name[month]]))) + "_" + str(year) + "_CO.csv")

def find_local_files(path,exts):
    file_list = []
    for root, dirs, files in os.walk(path):
        for currentFile in files:
            if any(currentFile.lower().endswith(ext) for ext in exts):
                file_list.append(os.path.join(root, currentFile))
    return file_list

def minute_species_average(df):
    spec_dict = {"CO":(4,[">=40","<=200"]),"CH4_dry":(2,[]),"CH4":(2,[]),"H2O":(3,[]),"CO2_dry":(1,[]),"CO2":(1,[])}
    df_list = []   
    for species,conds in spec_dict.items():
        eval_string = " & ".join(["(df['species'] == " + str(conds[0]) + ")"] + ["(df['" + species + "'] " + i + ")" for i in conds[1]])
        df_list.append(df.loc[eval(eval_string),species].resample("60S").mean())
    df = pd.concat(df_list,axis = 1)
    return df

def split_giant_df(df,skip_seconds = 60):
    df["AIR_CHANGE"] = (df["solenoid_valves"] != df["solenoid_valves"].shift()).cumsum()
    skip = np.timedelta64(skip_seconds,"s")

    air_df = df.loc[df["solenoid_valves"] == 2]
    air_df = air_df.groupby("AIR_CHANGE",group_keys = False).apply(lambda x: x.loc[(x.index.values[0] + skip):x.index.values[-1]])
    
    cal_df = df.loc[df["solenoid_valves"] != 2].drop(["AIR_CHANGE"],axis = 1)
    return air_df,cal_df

def create_cals(df,output_dir,year,skip_seconds = 60):
    spec_dict = {"CO":(4,[]),"CH4_dry":(2,[]),"CH4":(2,[]),"H2O":(3,[]),"CO2_dry":(1,[]),"CO2":(1,[])}
    valve_dict = {"_Zero_average":1,"_Cal_1":5,"_Cal_2":9}
    skip = np.timedelta64(skip_seconds,"s")
    df["valve_change"] = (df["solenoid_valves"] != df["solenoid_valves"].shift()).cumsum()
    df = df.loc[df["solenoid_valves"].isin([1,5,9])]
    df = df.groupby("valve_change",group_keys = False).apply(lambda x: x.loc[(x.index.values[0] + skip):x.index.values[-1]])
    grouped = df.groupby(["valve_change","species"])
    
    df_list = []
    mean_df = grouped.transform("mean").drop(["solenoid_valves"],axis = 1).join(df[["species","valve_change","solenoid_valves"]])
    sd_df = grouped.transform("std").drop(["solenoid_valves"],axis = 1).join(df[["species","valve_change","solenoid_valves"]])
    
    for species,conds in spec_dict.items():
        mean_spec_df = mean_df.loc[mean_df["species"] == conds[0]].drop_duplicates(subset = ["valve_change"],keep = "last")   
        sd_spec_df = sd_df.loc[sd_df["species"] == conds[0]].drop_duplicates(subset = ["valve_change"],keep = "last")
        for name,valve_pos in valve_dict.items():
            df_list.append(mean_spec_df.loc[mean_spec_df["solenoid_valves"] == valve_pos,species].rename(species + name + "_mean"))
            df_list.append(sd_spec_df.loc[sd_spec_df["solenoid_valves"] == valve_pos,species].rename(species + name + "_sd"))
    df = pd.concat(df_list,axis = 1)
    df = df.loc[df.index.year == year]
    
    #for x in pd.unique(df[df.index.year == i].index.month):
    #    create_folder([output_dir,i,calendar.month_name[x]])
    #    output_month_cal(df,output_dir,i,x)
    df.to_csv(os.path.join(output_dir,"CO_Cals_" + str(year) + ".csv"))

def output_month_cal(df,output_dir,year,month):
    df[((df.index.year == year) & (df.index.month == month))].to_csv(os.path.join(*list(map(str,[output_dir,year,calendar.month_name[month],calendar.month_name[month]]))) + "_" + str(year) + "_CO_cals.csv")




def output_errors(fpath,errors):
    try:
        with open(fpath) as err_file:
            err_list = [i.split("\t")[0] for i in err_file.readlines()]
    except:
        err_list = []
    errs = [i for i in errors if i.split("\t")[0] not in err_list]
    if len(errs):
        with open(fpath,"a") as err_file:
            for error in errs:
                err_file.write(error)
def split_df_months(df_list):
    month_year_list = []
    for df in df_list:
        month_year_list += list(pd.unique(zip(df.index.month,df.index.year)))
    month_year_list = list(set(month_year_list))
    split_by_months = []
    

def concat_them(flist):
    df_list = get_df_list(flist)
    error_list,df_list = split_df_list(df_list)
    species_df_list = []
    for i in [0,1,2,3]:
        species_df_list.append(pd.concat([x[i] for x in df_list]).sort_index())
    return species_df_list,error_list

def get_dupes(species_df_list):
    index_df = pd.concat([i["solenoid_valves"] for i in species_df_list])
    index_df_counts = index_df.index.value_counts()
    index_df_dupes = index_df[index_df_counts > 1]
    index_df = index_df[index_df_counts == 1]
    dupes = index_df_dupes.index.values
    txt_df = pd.to_datetime(dupes)
    return txt_df,index_df
    
def get_grouping_df(species_df_list,dupes):
    grouping_df = pd.concat([i.drop(dupes,errors = "ignore")["solenoid_valves"] for i in species_df_list],axis = 0).sort_index()
    grouping_df = grouping_df.to_frame()
    grouping_df["AIR_CHANGE"] = (grouping_df["solenoid_valves"] != grouping_df["solenoid_valves"].shift()).cumsum()
    grouping_df = grouping_df.loc[grouping_df["solenoid_valves"] == 2]
    return grouping_df

def get_drop_list(grouping_df,skip_seconds = 60):
    grouped = grouping_df.groupby("AIR_CHANGE")
    skip = datetime.timedelta(seconds = skip_seconds)
    drop_list = []
    for k,v in grouped.groups.items():
        drop_list.append((v[0],v[0] + skip))
    return drop_list


def drop_initial_air_times(species_df_list,drop_list):
    for num,full_df in enumerate(species_df_list):
        dropper = []
        for i in drop_list:
            dropper += list(full_df.loc[i[0]:i[1]].index.values)
        full_df = full_df.drop(dropper)
        species_df_list[num] = full_df
    return species_df_list

def split_me(species_df_list,ind_df):
    air_df = pd.concat([i.loc[i["solenoid_valves"] == 2].drop("solenoid_valves",axis = 1).resample("60s").mean() for i in species_df_list],axis = 1)
    cal_df = pd.concat([pd.concat([i.loc[i["solenoid_valves"] != 2].drop("solenoid_valves",axis = 1) for i in species_df_list],axis = 1),ind_df],axis = 1, join = "inner")
    return air_df,cal_df

def create_cals_2(cal_df,skip_seconds = 60):
    skip = np.timedelta64(skip_seconds,"s")
    valve_dict = {"_Zero_average":1,"_Cal_1":5,"_Cal_2":9}
    
    cal_df["valve_change"] = (cal_df["solenoid_valves"] != cal_df["solenoid_valves"].shift()).cumsum()
    cal_df = cal_df.loc[cal_df["solenoid_valves"].isin([1,5,9])]
    cal_df = cal_df.groupby("valve_change",group_keys = False).apply(lambda x: x.loc[(x.index.values[0] + skip):x.index.values[-1]])
    grouped = cal_df.groupby("valve_change")
    mean_df = grouped.transform("mean").drop("solenoid_valves",axis = 1).join(cal_df[["valve_change","solenoid_valves"]]).drop_duplicates(subset=["valve_change"],keep = "last")
    sd_df = grouped.transform("std").drop("solenoid_valves",axis = 1).join(cal_df[["valve_change","solenoid_valves"]]).drop_duplicates(subset=["valve_change"],keep = "last")
    df_list = []
    for to_add,valve_no in valve_dict.items():
        to_add_df = mean_df.loc[mean_df["solenoid_valves"] == valve_no].drop(["valve_change","solenoid_valves"],axis = 1)
        to_add_df.columns = [i + to_add + "_mean" for i in to_add_df.columns.values]
        df_list.append(to_add_df)
        to_add_df = sd_df.loc[sd_df["solenoid_valves"] == valve_no].drop(["valve_change","solenoid_valves"],axis = 1)
        to_add_df.columns = [i + to_add + "_SD" for i in to_add_df.columns.values]
        df_list.append(to_add_df)
    cal_df = pd.concat(df_list,axis = 1)
    return cal_df

def run_me(year = None):
    if year == None:
        year = int(datetime.datetime.now().year)
    curr_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    conf_dir = os.path.join(curr_dir,"config")
    data_dir = r"/gws/nopw/j04/ncas_obs/cvao/raw_data/ncas-co-picarro-1/data/UserData/DataLog_User"#os.path.join(parent_dir,"Input_data")
    output_dir = r"/gws/nopw/j04/ncas_obs/amf/processing/ncas-co-picarro-1/output"#os.path.join(parent_dir,"Output")    
    excluded_dates = read_conf_file(os.path.join(conf_dir,"remove_date_times.mww"))
    exc_dates_list = list(zip(excluded_dates["Start Date"].values,excluded_dates["End Date"].values))
    bad_files = read_conf_file_list(os.path.join(conf_dir,"bad_files.mww"),"\t",0)

    flist = find_local_files(os.path.join(data_dir,str(year)),[".dat"])
    flist += find_local_files(os.path.join(data_dir,str(year-1),"12"),[".dat"])
    flist += find_local_files(os.path.join(data_dir,str(year+1),"01"),[".dat"])
    
    flist = filter_flist(flist,bad_files,year)
    species_df_list,error_list = concat_them(flist)

    species_df_list = drop_initial_air_times(species_df_list,exc_dates_list)
    dupes,ind_df = get_dupes(species_df_list)
    grouping_df = get_grouping_df(species_df_list,dupes)
    drop_list = get_drop_list(grouping_df)
    species_df_list = drop_initial_air_times(species_df_list,drop_list)
    air_df,cal_df = split_me(species_df_list,ind_df)
    del(species_df_list)
    cal_df = create_cals_2(cal_df)

    output_df(air_df,output_dir,year)
    cal_df.to_csv(os.path.join(output_dir,"CO_Cals_" + str(year) + ".csv"))
    if len(error_list) > 0:
        output_errors(os.path.join(output_dir,str(year),"file_errors.txt"),error_list)


if __name__ == "__main__":
    run_me()
    
