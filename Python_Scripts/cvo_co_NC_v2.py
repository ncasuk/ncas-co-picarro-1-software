import os
import numpy as np

def cvo_co_get_meta_v1(fn):
   import csv
   meta = []
   
   str3 = "platform_location"
   str4 = "platform_height"
   
   ifile = open(fn,'rU')
   reader = csv.reader(ifile, delimiter = chr(9))

   for row in reader:
      if len(row) > 0:  
         xx = str(row[0])
         yy = str(row[1])
         meta.append(yy)
#Could just add text rather than looking in the files
         #extract loaction
         ix3 = xx.find(str3)
         if ix3 > -1:
            xxx = yy
            ix31 = xxx.find(" (")
            ix32 = xxx.find(") ")
            lat = float(xxx[ix31+2:ix32])
            yyy = xxx[ix32+1:len(xxx)]
            ix41 = yyy.find("(")
            lon = float(yyy[ix41+1:len(yyy)-1])
         #extract platform height
         ix4 = xx.find(str4)
         if ix4 > -1:
            ix5 = yy.find("m")
            if ix5 > -1:
               plat_Z = float(yy[0:ix5])
            if ix5 < 0:
               plat_Z = float(yy)
      
   ifile.close()
   meta.append(lat)
   meta.append(lon)
   meta.append(plat_Z)
   
   return meta
   
def cvo_co_create_NC_file_v1(meta, dout, DT):
   from netCDF4 import Dataset
   
   f1 = meta[0] #instrument name
   f2 = meta[29] #platform name
   mm = str(int(DT[0,1]))
   if len(mm)<2:
      mm = "0" + mm
   dd = str(int(DT[0,2]))
   if len(dd)<2:
       dd='0' + dd 
   f3 = str(int(DT[0,0])) + mm + dd #date
   f4 = meta[1] #data product
   f5 = meta[20] #version number
   f6 = ".nc"
   #fn = dout + f1 + chr(95) + f2 + chr(95) + f3 + chr(95) + f4 + chr(95) + f5 + f6
   fn = os.path.join( dout, f1 + chr(95) + f2 + chr(95) + f3 + chr(95) + f4 + chr(95) + f5 + f6)
   
   nc = Dataset(fn, "w",  format = "NETCDF4_CLASSIC") 
   
   return nc
   
def cvo_co_NC_Global_Attributes_v1(nc, meta, ET):
   from datetime import datetime
   import pytz
   #could just add all the detail in here rather than refer to file
   nc.Conventions = meta[2]
   nc.source = meta[3]
   nc.instrument_manufacturer = meta[4]
   nc.instrument_model = meta[5]
   nc.instrument_serial_number = meta[6]
   nc.instrument_software = meta[7]
   nc.instrument_software_version = meta[8]
   nc.creator_name = meta[9]
   nc.creator_email = meta[10]
   nc.creator_url = meta[11]
   nc.institution = meta[12]
   nc.processing_software_url = meta[13]
   nc.processing_software_version = meta[14]
   nc.calibration_sensitivity = meta[15]
   nc.calibration_certification_date = meta[16]
   nc.calibration_certification_url = meta[17]
   nc.sampling_interval = meta[18]
   nc.averaging_interval = meta[19]
   nc.product_version = meta[20]
   nc.processing_level = int(meta[21])
   nc.last_revised_date = datetime.now().isoformat()
   nc.project = meta[23]
   nc.project_principal_investigator = meta[24]
   nc.project_principal_investigator_email = meta[25]
   nc.project_principal_investigator_url = meta[26]
   nc.licence = meta[27]
   nc.acknowledgement = meta[28]
   nc.platform = meta[29]
   nc.platform_type = meta[30]
   nc.deployment_mode = meta[31]
   nc.title = meta[32]
   nc.featureType = meta[33]
   nc.time_coverage_start = datetime.fromtimestamp(ET[0]).isoformat()
   nc.time_coverage_end = datetime.fromtimestamp(ET[len(ET)-1]).isoformat()
   nc.geospatial_bounds = meta[36]
   nc.platform_location = meta[37]
   nc.platform_altitude = meta[38]
   nc.location_keywords = meta[39]
   nc.amf_vocabularies_release = meta[40]
   nc.history = meta[41]
   nc.comment = meta[42]
   #flags
   nc.qc_flag_comment = meta[45]
   nc.qc_flag_value_0_description = meta[46]
   nc.qc_flag_value_1_description = meta[47]
   nc.qc_flag_value_1_assessment = meta[48]
   nc.qc_flag_value_2_description = meta[49]
   nc.qc_flag_value_2_assessment = meta[50]
   nc.qc_flag_value_3_description = meta[51]
   nc.qc_flag_value_3_assessment = meta[52]
   
def cvo_co_NC_Dimensions_v1(nc, ET):
   time = nc.createDimension('time', len(ET) )
   latitude = nc.createDimension('latitude', 1)
   longitude = nc.createDimension('longitude', 1) 
   
def cvo_co_NC_VaraiblesAndData_v1(nc, meta, data):
   #time
   times = nc.createVariable('time', np.double, ('time',))
   #time variable attributes
   #times.dimension = 'time'
   #times.type = 'double'
   times.units = 'seconds since 1970-01-01 00:00:00'
   times.standard_name = 'time'
   times.long_name = 'Time (seconds since 1970-01-01 00:00:00)'
  #for plotting through netCDF browser
   times.axis = 'T'
   times.valid_min = min(data.ET)
   times.valid_max = max(data.ET)
   times.calendar = 'standard'
   #write data
   times[:] = data.ET
   
   #lat
   latitudes = nc.createVariable('latitude', np.double, ('latitude',))
   #latitude variable attributes
   latitudes.dimension = 'latitude'
   latitudes.type = 'float'
   latitudes.units = 'degrees_north'
   latitudes.standard_name = 'latitude'
   latitudes.long_name = 'Latitude'
   latitudes.calendar = 'standard'
   #write data
   latitudes[:] = float(meta[len(meta)-3])
   
   #lon
   longitudes = nc.createVariable('longitude', np.double, ('longitude',))
   #longitude variable attributes
   longitudes.dimension = 'longitude'
   longitudes.type = 'float'
   longitudes.units = 'degrees_east'
   longitudes.standard_name = 'longitude'
   longitudes.long_name = 'Longitude'
   longitudes.calendar = 'standard'
   #write data
   longitudes = float(meta[len(meta)-2])
   
   #doy
   doys = nc.createVariable('day_of_year', np.double, ('time',))
   #day_of_year variable attributes
   doys.dimension = 'time'
   doys.type = 'float'
   doys.units = '1'
   doys.long_name = 'Day of Year'
   doys.valid_min = 1
   doys.valid_max = 367
   #write data
   doys[:] = data.DoY
   
   #year
   years = nc.createVariable('year', 'f4', ('time',))
   #year variable attributes
   years.dimension = 'time'
   years.type = 'int'
   years.units = '1'
   years.long_name = 'Year'
   years.valid_min = 1900
   years.valid_max = 2100 
   #write data
   years[:] = data.DT[:,0]
   
   #month
   months = nc.createVariable('month', 'f4', ('time',))
   #month variable attributes
   months.dimension = 'time'
   months.type = 'int'
   months.units = '1'
   months.long_name = 'Month'
   months.valid_min = 1
   months.valid_max = 12 
   #write data
   months[:] = data.DT[:,1]
   
   #day
   days = nc.createVariable('day', 'f4', ('time',))
   #day variable attributes
   days.dimension = 'time'
   days.type = 'int'
   days.units = '1'
   days.long_name = 'Day'
   days.valid_min = 1
   days.valid_max = 31 
   #write data
   days[:] = data.DT[:,2]
   
   #hour
   hours = nc.createVariable('hour', 'f4', ('time',))
   #hour variable attributes
   hours.dimension = 'time'
   hours.type = 'int'
   hours.units = '1'
   hours.long_name = 'Hour'
   hours.valid_min = 0
   hours.valid_max = 23 
   #write data
   hours[:] = data.DT[:,3]
   
   #minute
   minutes = nc.createVariable('minute', 'f4', ('time',))
   #minute variable attributes
   minutes.dimension = 'time'
   minutes.type = 'int'
   minutes.units = '1'
   minutes.long_name = 'Minute'
   minutes.valid_min = 0
   minutes.valid_max = 59 
   #write data
   minutes[:] = data.DT[:,4]
   
   #second
   seconds = nc.createVariable('second', np.double, ('time',))
   #second variable attributes
   seconds.dimension = 'time'
   seconds.type = 'float'
   seconds.units = '1'
   seconds.long_name = 'Second'
   seconds.valid_min = 0
   seconds.valid_max = 59.99999 
   #write data
   seconds[:] = data.DT[:,5]
   
   #CO conc
   CO = nc.createVariable('co_concentration_in_air', np.double, ('time',),fill_value=-1.00e+20)
   #bks variable attributes
   CO.dimension = 'time'
   CO.type = 'float32'
   CO.units = 'ppb'
   CO.long_name = 'CO concentration in air'
   CO.valid_min = 0
   CO.valid_max = 500
   CO.cell_methods = 'time:point'
   CO.coordinates = 'latitude longitude'
   #write data
   CO[:] = data.CO
   
   #Qc flag
   qc_flags = nc.createVariable('qc_flag', 'f4', ('time',))
   #qc_flag variable attribute
   qc_flags.dimension = 'time'
   qc_flags.type = 'byte'
   qc_flags.units = '1'
   qc_flags.long_name = 'Data Quality flag'
   qc_flags.valid_min = 1
   qc_flags.valid_max = 3
   #write data
   qc_flags[:] = data.flag
   qc_flags.flag_values = [0, 1, 2, 3]
   #qc_flags.flag_meanings = f"{meta[45]},{meta[46]},{meta[48]},{meta[50]}"
   qc_flags.flag_meanings = " ".join([meta[46], meta[47], meta[49], meta[51]])
   
   
def NC_cvo_co_v1(meta, dout, data):
   nc = cvo_co_create_NC_file_v1(meta, dout, data.DT)
   
   cvo_co_NC_Global_Attributes_v1(nc, meta, data.ET)
   cvo_co_NC_Dimensions_v1(nc, data.ET)
   cvo_co_NC_VaraiblesAndData_v1(nc, meta, data)

   nc.close()