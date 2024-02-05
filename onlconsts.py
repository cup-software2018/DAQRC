kSOFTWARE_VER = '3.1.2'
kONLDAQ_DIR = '/amore2/onldaq/dev_3.2.0'
kRAWDATA_DIR = '/amore2/amore2test'
kRUNCATALOGDBFILE = '/amore2/amore2test/runcatalog.db'
kDEFAULTCONFIGDIR = '/amore2/onldaq/config'

kRUNTYPELIST = ['', 'physics', 'muon', 'calibration', 'test', 'pedestal']

kDAQSERVER_IP = '172.16.2.50'
kDAQSERVER_PORT = 7809
kDAQSERVER_ADDR = (kDAQSERVER_IP, kDAQSERVER_PORT)


#
# Do not modify from here!!!
#
kEXESCRIPT = 'executedaq.sh'
kMESSLEN = 32

# Commands
kCONFIGRUN = 1
kSTARTRUN = 2
kENDRUN = 3
kEXIT = 4
kQUERYDAQSTATUS = 10
kQUERYRUNINFO = 12
kQUERYTRGINFO = 14
kQUERYMONITOR = 21

# RUN Status
kDOWN = 0
kBOOTED = 1
kCONFIGURED = 2
kRUNNING = 3
kRUNENDED = 4
kPROCENDED = 5
kWARNING = 6
kERROR = 7

kDAQSTATE = ['Down', 'Booted', 'Configured',
             'Running', 'RunEnd', 'RunEnd', '', 'Error']
