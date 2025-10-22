# args exected
#1. full path to file to be converted
#2. output diretory 
#3. path to meta file

import sys
#import os
import numpy as np
from collections import namedtuple
from cvo_co_parser_v1 import cvo_co_get_file_v1, cvo_co_parse_data_v1
from cvo_co_NC_v2 import cvo_co_get_meta_v1, NC_cvo_co_v1
import argparse

parser = argparse.ArgumentParser(description="CVAO Carbon monoxide data parser")
parser.add_argument('-o', '--outdir', default='./processed', help='Directory for output NetCDF files', dest='dout')
parser.add_argument('datafiles', nargs='+')

args = parser.parse_args()

#path to file out
dout = args.dout
data = namedtuple("data", "DT DoY ET CO flag")
#path to file in
for fin in args.datafiles:

    #"/AMF_Netcdf/"
    #path to file metadata
    #fn_meta = str(sys.argv[3])
    fn_meta = fin[0:-12]+'meta_'+fin[-12:]

    #get the data
    data.DT, data.DoY, data.ET, data.CO, data.flag = cvo_co_get_file_v1(fin,np)

    #parse data - make sure just one month of data
    data.DT, data.DoY, data.ET, data.CO, data.flag = cvo_co_parse_data_v1(data)
 
    #save nc file
    meta = cvo_co_get_meta_v1(fn_meta)
    NC_cvo_co_v1(meta, dout, data)








