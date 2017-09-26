#!/usr/local/bin/python3.6
import subprocess
from dbConn import orcl
import os


tables = ['*******']
ggHome = '/u01/app/oracle/product/12.2.0.1/ogg1'
ggsciCommand = '/u01/app/oracle/product/12.2.0.1/ogg1/ggsci'
defgenCmd = '/u01/app/oracle/product/12.2.0.1/ogg1/defgen'
defsFile = ggHome + '/dirdef/PC90TSTB'
extractName = 'EXT_77'
replicatName = 'REP_77'
pumpName = ''
sourceDB = '********'
destDB = '*******'
defgenParam = '/tmp/defgen'
extractParam = ggHome + '/dirprm/ext_77.prm'
replicatParam = ggHome + '/dirprm/rep_77.prm'
sdsUser = 'SIA_SDS_PSFT_9_0_77'
fExpParFile = '/tmp/exp_par'
fImpParFile = '/tmp/imp_par'
srcLogin = '@' + sourceDB
dstLogin = '@' + destDB
dstDWUser = '******'

def stopGoldenGate(param1):
  ec = subprocess.Popen(('echo', 'STOP ' + param1), stdout=subprocess.PIPE)
  gg = subprocess.Popen(ggsciCommand, stdin=ec.stdout)
  gg.wait()

def startGoldenGate(param1):
  ec = subprocess.Popen(('echo', 'START ' + param1), stdout=subprocess.PIPE)
  gg = subprocess.Popen(ggsciCommand, stdin=ec.stdout)
  gg.wait()

def getCurrentSCN():
  with orcl('system', 'qzLmDtCHb3', 'PC90TSTB') as db:
    sql = 'SELECT APPLIED_SCN FROM DBA_CAPTURE WHERE CAPTURE_NAME = \'OGG$CAP_'  + extractName + '\''
    data = db.dbExecuteFetchOne(sql)
    value = (data[0])
    return value

def myTables():
  with orcl('system', 'qzLmDtCHb3', 'PC90TSTB') as db:
    # Truncate the table
    sql = 'TRUNCATE TABLE SYSTEM.MY_TABLES'
    db.dbExecuteCommand(sql)
    # Populate with new tables
    for tab in tables:
      sql = 'INSERT INTO SYSTEM.MY_TABLES (tab) VALUES (\'' + tab + '\')'
      db.dbExecuteCommand(sql)
      db.dbCommit()

def writeExtractFile(param1):
  with open(extractParam, 'a') as f:
    f.write('\n')
    f.write('TABLE SYSADM.' + param1 + ';')

def writeReplicatFile(param1):
  with open(replicatParam, 'a') as f:
    f.write('------------------------------------------------\n')
    f.write('--   ' + param1 + '\n')
    f.write('------------------------------------------------\n')

    f.write('-- Process Inserts --\n')

    f.write('GETINSERTS\n')
    f.write('IGNOREUPDATES\n')
    f.write('IGNOREDELETES\n')
    f.write('MAP SYSADM.' + param1 + ', TARGET ' + sdsUser + '.' + param1 + ',\n')
    f.write('COLMAP (USEDEFAULTS,\n')
    f.write('CDC$_RPL_LAST_UPDATE_DATE = @DATENOW (),\n')
    f.write('CDC$_SRC_LAST_UPDATE_DATE = @GETENV (\'GGHEADER\', \'COMMITTIMESTAMP\'),\n')
    f.write('CDC$_DML_CODE = \'I\')\n')
    f.write(';\n')
    f.write('\n')
    f.write('-- Process Updates --\n')
    f.write('\n')
    f.write('GETUPDATES\n')
    f.write('IGNOREINSERTS\n')
    f.write('IGNOREDELETES\n')
    f.write('MAP SYSADM.' + param1 + ', TARGET ' + sdsUser + '.' + param1 + ',\n')
    f.write('COLMAP (USEDEFAULTS,\n')
    f.write('CDC$_RPL_LAST_UPDATE_DATE = @DATENOW (),\n')
    f.write('CDC$_SRC_LAST_UPDATE_DATE = @GETENV (\'GGHEADER\', \'COMMITTIMESTAMP\'),\n')
    f.write('CDC$_DML_CODE = 'U')\n')
    f.write(';\n')
    f.write('\n')
    f.write('-- Process Deletes --\n')
    f.write('\n')
    f.write('IGNOREINSERTS\n')
    f.write('IGNOREUPDATES\n')
    f.write('GETDELETES\n')
    f.write('UPDATEDELETES\n')
    f.write('MAP SYSADM.' + param1 + ', TARGET ' + sdsUser + '.' + param1 + ',\n')
    f.write('COLMAP (USEDEFAULTS,\n')
    f.write('CDC$_RPL_LAST_UPDATE_DATE = @DATENOW (),\n')
    f.write('CDC$_SRC_LAST_UPDATE_DATE = @GETENV (\'GGHEADER\', \'COMMITTIMESTAMP\'),\n')
    f.write('CDC$_DML_CODE = \'D\')\n')
    f.write(';\n')
    f.write('\n')

def createExpParFile(param1):
  with open(fExpParFile, 'w') as f:
    f.write('DIRECTORY=NFSDIR\n')
    f.write('DUMPFILE=dump_tsb_%u.dmp\n')
    f.write('PARALLEL=4\n')
    f.write('FLASHBACK_SCN=' + param1 + '\n')
    f.write('SCHEMAS=SYSADM\n')
    f.write('INCLUDE=TABLE:"IN(SELECT tab FROM MY_TABLES)"\n')

def createImpParFile():
  with open(fImpParFile, 'w') as f:
    f.write('DIRECTORY=NFSDUMP\n')
    f.write('DUMPFILE=dump_tsb_%u.dmp\n')
    f.write('PARALLEL=4\n')
    f.write('TABLE_EXISTS_ACTION=REPLACE\n')
    f.write('REMAP_SCHEMA=SYSADM:' + sdsUser + '\n')

def alterGrantDestTables(tab):
  with orcl('system', 'RzgMEuUd8S', 'OBINPD') as db:
    sql = 'ALTER TABLE '
    sql = sql + sdsUser + '.' + tab
    sql = sql + ' add (CDC$_SRC_LAST_UPDATE_DATE TIMESTAMP  default CURRENT_TIMESTAMP,CDC$_RPL_LAST_UPDATE_DATE TIMESTAMP default CURRENT_TIMESTAMP, CDC$_DML_CODE VARCHAR2(1 CHAR) default \'I\') '
    print(sql)
    db.dbExecuteCommand(sql)
    
    # Grant SELECT to DW user
    sql = 'GRANT SELECT ON ' + sdsUser + '.' + tab + ' TO ' + dstDWUser
    db.dbExecuteCommand(sql)



### BEGIN MAIN PROGRAM ###
def main():
  # Change to the GoldenGate Home for credentialstore access
  os.chdir(ggHome)

  # Stop the GG extract and replicat
  stopGoldenGate(extractName)
  stopGoldenGate(replicatName)

  # get the last captured SCN for the extract not that it is stopped
  currSCN = getCurrentSCN()
  currSCN = str(currSCN)

  # write the defgen param file
  with open(defgenParam, 'w') as f:
    f.write('DEFSFILE ' + defsFile + ', FORMAT RELEASE 12.1, APPEND\n')
    f.write('USERIDALIAS oggtstb')
    f.write('\n')
    for tab in tables:
      f.write('TABLE SYSADM.' + tab + ';\n')

  # Generate the table definitions
  subprocess.run((defgenCmd, 'PARAMFILE', defgenParam))

  # Add lines to param files
  for tab in tables:
    writeReplicatFile(tab)
    writeExtractFile(tab)

  # Sync the data
  # Start by adding the tables to SYSTEM.MY_TABLES
  # This is the table I use for expdp paramter file
  myTables()

  # Create the expdp and impdp parfile
  createExpParFile(currSCN)
  createImpParFile()

  # Run the expdp
  subprocess.run(('expdp', srcLogin,'parfile=' + fExpParFile))

  # Run the impdp
  subprocess.run(('impdp', dstLogin,'parfile=' + fImpParFile))

  # Execute table ALTER statements
  for tab in tables:
    alterGrantDestTables(tab)

  # Start the GG Extract and Replicat
  startGoldenGate(replicatName)
  startGoldenGate(extractName)


main()
