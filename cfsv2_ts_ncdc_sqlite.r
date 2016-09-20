###########################################
# cfsv2_ts_ncdc_sqlite.r
# processes grib2 files from ncdc cfsv2 archive
# saves data to sqlite database
# saves data in rdata format
###########################################

library(data.table)
library(dplyr)
library(ggplot2)
library(lubridate)
library(stringr)
library(tidyr)
library(RSQLite)
library(sqldf)

dir_scratch = '/d1/dbroman/projects/cfsv2/scratch/'
dir_gbm = '/d1/dbroman/projects/cfsv2/grib2/gbm/'
dir_africa = '/d1/dbroman/projects/cfsv2/grib2/africa/'

dir_gbm_proc = '/d1/dbroman/projects/cfsv2/rdata/gbm/'
dir_africa_proc = '/d1/dbroman/projectscfsv2/rdata/africa/'

setwd(dir_gbm_proc)
db = dbConnect(SQLite(), dbname = 'gbm_cfsv2.sqlite')

gbm_file_list = list.files(dir_gbm, pattern = '*.grb2')
nfiles_gbm = length(gbm_file_list)

for(i in 1:nfiles_gbm){
  gbm_file_sel = paste0(dir_gbm, gbm_file_list[i])
	system(paste0("wgrib2 ", gbm_file_sel, " -csv temp.csv"))
	tryCatch({
	fcst_dt_temp = fread('temp.csv') %>% setnames(c('date_init', 'date_fcst', 'var', 'level', 'lon', 'lat', 'value'))
	})
	var_temp = unique(fcst_dt_temp$var)
  dbWriteTable(conn = db, name = var_temp, value = fcst_dt_temp, row.names = F, append = T)
}
