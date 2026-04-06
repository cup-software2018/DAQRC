# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'rc.ui'
##
## Created by: Qt User Interface Compiler version 6.6.3
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PySide6.QtCore import (QCoreApplication, QDate, QDateTime, QLocale,
    QMetaObject, QObject, QPoint, QRect,
    QSize, QTime, QUrl, Qt)
from PySide6.QtGui import (QBrush, QColor, QConicalGradient, QCursor,
    QFont, QFontDatabase, QGradient, QIcon,
    QImage, QKeySequence, QLinearGradient, QPainter,
    QPalette, QPixmap, QRadialGradient, QTransform)
from PySide6.QtWidgets import (QApplication, QComboBox, QFormLayout, QGroupBox,
    QHBoxLayout, QLabel, QLineEdit, QMainWindow,
    QPushButton, QSizePolicy, QTextBrowser, QTextEdit,
    QVBoxLayout, QWidget)

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        if not MainWindow.objectName():
            MainWindow.setObjectName(u"MainWindow")
        MainWindow.resize(800, 1200)
        sizePolicy = QSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(MainWindow.sizePolicy().hasHeightForWidth())
        MainWindow.setSizePolicy(sizePolicy)
        MainWindow.setMinimumSize(QSize(800, 1200))
        MainWindow.setMaximumSize(QSize(800, 1200))
        self.centralwidget = QWidget(MainWindow)
        self.centralwidget.setObjectName(u"centralwidget")
        self.layoutWidget = QWidget(self.centralwidget)
        self.layoutWidget.setObjectName(u"layoutWidget")
        self.layoutWidget.setGeometry(QRect(10, 20, 781, 1170))
        self.verticalLayout_3 = QVBoxLayout(self.layoutWidget)
        self.verticalLayout_3.setObjectName(u"verticalLayout_3")
        self.verticalLayout_3.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout = QHBoxLayout()
        self.horizontalLayout.setObjectName(u"horizontalLayout")
        self.groupBox = QGroupBox(self.layoutWidget)
        self.groupBox.setObjectName(u"groupBox")
        self.groupBox.setMinimumSize(QSize(0, 400))
        font = QFont()
        font.setPointSize(14)
        self.groupBox.setFont(font)
        self.groupBox.setAlignment(Qt.AlignCenter)
        self.layoutWidget_2 = QWidget(self.groupBox)
        self.layoutWidget_2.setObjectName(u"layoutWidget_2")
        self.layoutWidget_2.setGeometry(QRect(6, 34, 371, 334))
        self.verticalLayout_2 = QVBoxLayout(self.layoutWidget_2)
        self.verticalLayout_2.setObjectName(u"verticalLayout_2")
        self.verticalLayout_2.setContentsMargins(0, 0, 0, 0)
        self.formLayout = QFormLayout()
        self.formLayout.setObjectName(u"formLayout")
        self.label = QLabel(self.layoutWidget_2)
        self.label.setObjectName(u"label")
        font1 = QFont()
        font1.setPointSize(10)
        self.label.setFont(font1)
        self.label.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.formLayout.setWidget(0, QFormLayout.LabelRole, self.label)

        self.ShiftConfig = QLineEdit(self.layoutWidget_2)
        self.ShiftConfig.setObjectName(u"ShiftConfig")
        self.ShiftConfig.setMinimumSize(QSize(0, 36))
        font2 = QFont()
        font2.setPointSize(11)
        self.ShiftConfig.setFont(font2)

        self.formLayout.setWidget(0, QFormLayout.FieldRole, self.ShiftConfig)

        self.label_2 = QLabel(self.layoutWidget_2)
        self.label_2.setObjectName(u"label_2")
        self.label_2.setFont(font1)
        self.label_2.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.formLayout.setWidget(1, QFormLayout.LabelRole, self.label_2)

        self.RunTypeConfig = QComboBox(self.layoutWidget_2)
        self.RunTypeConfig.setObjectName(u"RunTypeConfig")
        self.RunTypeConfig.setMinimumSize(QSize(0, 36))
        self.RunTypeConfig.setFont(font2)

        self.formLayout.setWidget(1, QFormLayout.FieldRole, self.RunTypeConfig)

        self.label_3 = QLabel(self.layoutWidget_2)
        self.label_3.setObjectName(u"label_3")
        self.label_3.setFont(font1)
        self.label_3.setAlignment(Qt.AlignRight|Qt.AlignTrailing|Qt.AlignVCenter)

        self.formLayout.setWidget(2, QFormLayout.LabelRole, self.label_3)

        self.RunDescConfig = QTextEdit(self.layoutWidget_2)
        self.RunDescConfig.setObjectName(u"RunDescConfig")
        self.RunDescConfig.setFont(font2)

        self.formLayout.setWidget(2, QFormLayout.FieldRole, self.RunDescConfig)


        self.verticalLayout_2.addLayout(self.formLayout)

        self.ConfigFileButton = QPushButton(self.layoutWidget_2)
        self.ConfigFileButton.setObjectName(u"ConfigFileButton")
        self.ConfigFileButton.setMinimumSize(QSize(0, 40))
        self.ConfigFileButton.setFont(font2)

        self.verticalLayout_2.addWidget(self.ConfigFileButton)

        self.ConfigFileLabel = QLabel(self.layoutWidget_2)
        self.ConfigFileLabel.setObjectName(u"ConfigFileLabel")
        self.ConfigFileLabel.setFont(font1)
        self.ConfigFileLabel.setAlignment(Qt.AlignCenter)

        self.verticalLayout_2.addWidget(self.ConfigFileLabel)


        self.horizontalLayout.addWidget(self.groupBox)

        self.runControlBox = QGroupBox(self.layoutWidget)
        self.runControlBox.setObjectName(u"runControlBox")
        self.runControlBox.setMinimumSize(QSize(0, 400))
        self.runControlBox.setFont(font)
        self.runControlBox.setAlignment(Qt.AlignCenter)
        self.layoutWidget_3 = QWidget(self.runControlBox)
        self.layoutWidget_3.setObjectName(u"layoutWidget_3")
        self.layoutWidget_3.setGeometry(QRect(11, 34, 361, 341))
        self.verticalLayout = QVBoxLayout(self.layoutWidget_3)
        self.verticalLayout.setObjectName(u"verticalLayout")
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)
        self.BootButton = QPushButton(self.layoutWidget_3)
        self.BootButton.setObjectName(u"BootButton")
        sizePolicy.setHeightForWidth(self.BootButton.sizePolicy().hasHeightForWidth())
        self.BootButton.setSizePolicy(sizePolicy)
        self.BootButton.setMinimumSize(QSize(0, 40))
        self.BootButton.setMaximumSize(QSize(1000, 40))
        self.BootButton.setFont(font2)

        self.verticalLayout.addWidget(self.BootButton)

        self.ConfigButton = QPushButton(self.layoutWidget_3)
        self.ConfigButton.setObjectName(u"ConfigButton")
        sizePolicy.setHeightForWidth(self.ConfigButton.sizePolicy().hasHeightForWidth())
        self.ConfigButton.setSizePolicy(sizePolicy)
        self.ConfigButton.setMinimumSize(QSize(0, 40))
        self.ConfigButton.setMaximumSize(QSize(1000, 40))
        self.ConfigButton.setFont(font2)

        self.verticalLayout.addWidget(self.ConfigButton)

        self.StartButton = QPushButton(self.layoutWidget_3)
        self.StartButton.setObjectName(u"StartButton")
        sizePolicy.setHeightForWidth(self.StartButton.sizePolicy().hasHeightForWidth())
        self.StartButton.setSizePolicy(sizePolicy)
        self.StartButton.setMinimumSize(QSize(0, 40))
        self.StartButton.setMaximumSize(QSize(1000, 40))
        self.StartButton.setFont(font2)

        self.verticalLayout.addWidget(self.StartButton)

        self.EndButton = QPushButton(self.layoutWidget_3)
        self.EndButton.setObjectName(u"EndButton")
        sizePolicy.setHeightForWidth(self.EndButton.sizePolicy().hasHeightForWidth())
        self.EndButton.setSizePolicy(sizePolicy)
        self.EndButton.setMinimumSize(QSize(0, 40))
        self.EndButton.setMaximumSize(QSize(1000, 40))
        self.EndButton.setFont(font2)

        self.verticalLayout.addWidget(self.EndButton)

        self.ExitButton = QPushButton(self.layoutWidget_3)
        self.ExitButton.setObjectName(u"ExitButton")
        sizePolicy.setHeightForWidth(self.ExitButton.sizePolicy().hasHeightForWidth())
        self.ExitButton.setSizePolicy(sizePolicy)
        self.ExitButton.setMinimumSize(QSize(0, 40))
        self.ExitButton.setMaximumSize(QSize(1000, 40))
        self.ExitButton.setFont(font2)

        self.verticalLayout.addWidget(self.ExitButton)


        self.horizontalLayout.addWidget(self.runControlBox)


        self.verticalLayout_3.addLayout(self.horizontalLayout)

        self.RunStatsBox = QGroupBox(self.layoutWidget)
        self.RunStatsBox.setObjectName(u"RunStatsBox")
        self.RunStatsBox.setMinimumSize(QSize(0, 380))
        self.RunStatsBox.setMaximumSize(QSize(16777215, 381))
        self.RunStatsBox.setFont(font)
        self.RunStatsBox.setAlignment(Qt.AlignCenter)

        self.verticalLayout_3.addWidget(self.RunStatsBox)

        self.LogBox = QGroupBox(self.layoutWidget)
        self.LogBox.setObjectName(u"LogBox")
        self.LogBox.setFont(font)
        self.LogBox.setAlignment(Qt.AlignCenter)
        self.verticalLayout_log = QVBoxLayout(self.LogBox)
        self.verticalLayout_log.setObjectName(u"verticalLayout_log")
        self.LogViewer = QTextBrowser(self.LogBox)
        self.LogViewer.setObjectName(u"LogViewer")
        self.LogViewer.setFont(font1)

        self.verticalLayout_log.addWidget(self.LogViewer)


        self.verticalLayout_3.addWidget(self.LogBox)

        MainWindow.setCentralWidget(self.centralwidget)

        self.retranslateUi(MainWindow)

        QMetaObject.connectSlotsByName(MainWindow)
    # setupUi

    def retranslateUi(self, MainWindow):
        MainWindow.setWindowTitle(QCoreApplication.translate("MainWindow", u"rc", None))
        self.groupBox.setTitle(QCoreApplication.translate("MainWindow", u"Run Configuration", None))
        self.label.setText(QCoreApplication.translate("MainWindow", u"Shift", None))
        self.label_2.setText(QCoreApplication.translate("MainWindow", u"Run Type", None))
        self.label_3.setText(QCoreApplication.translate("MainWindow", u"Run Desc", None))
        self.ConfigFileButton.setText(QCoreApplication.translate("MainWindow", u"Config File", None))
        self.ConfigFileLabel.setText("")
        self.runControlBox.setTitle(QCoreApplication.translate("MainWindow", u"Run Control", None))
        self.BootButton.setText(QCoreApplication.translate("MainWindow", u"Boot", None))
        self.ConfigButton.setText(QCoreApplication.translate("MainWindow", u"Config Run", None))
        self.StartButton.setText(QCoreApplication.translate("MainWindow", u"Start Run", None))
        self.EndButton.setText(QCoreApplication.translate("MainWindow", u"End Run", None))
        self.ExitButton.setText(QCoreApplication.translate("MainWindow", u"Exit", None))
        self.RunStatsBox.setTitle(QCoreApplication.translate("MainWindow", u"Run Statistics", None))
        self.LogBox.setTitle(QCoreApplication.translate("MainWindow", u"System Logs", None))
    # retranslateUi

