from datetime import datetime

WORK_MODE_MAP = {
    0: "cWaitMode - 等待、自檢模式",
    1: "cNormalMode - 發電模式",
    2: "cFaultMode - 錯誤模式",
}

# Function Bit (功能狀態位) 對應表
FUNCTION_BIT_MAP = {
    15: "High Impedance Flag - 高阻抗標誌",
    13: "Ground Fault Flag - 接地故障標誌 (NG/OK)",
    12: "Battery Activation - 電池激活功能 (ON/OFF)",
    11: "Anti-Reverse Flow - 防逆流 (ON/OFF)",
    10: "EMS Mode - EMS 模式 (ON/OFF)",
    9: "Battery Auto Management - 電池自動管理模式 (ON/OFF)",
    8: "Meter - 電表 (OK/NG)",
    7: "MPPT for Shadow - MPPT 陰影掃描 (ON/OFF)",
    3: "Power Limit Function - 功率限制功能 (ON/OFF)",
    2: "Burn-in Mode - 老化模式 (ON/OFF)",
    1: "LVRT - 高低穿功能 (ON/OFF)",
    0: "Anti-Islanding - 孤島保護 (ON/OFF)",
}

# GPRS Burn-in Mode (GPRS 老化模式) 對應表
GPRS_BURN_IN_MODE_MAP = {
    0: "Normal Mode - 正常模式",
    1: "Burn-in Mode - 老化模式",
}

class GoodWe:
    ERROR_SEVERITY = {
        "severe": {2147483648, 1073741824, 536870912, 268435456},  # 嚴重警告
        "high": {134217728, 67108864, 33554432, 16777216, 8388608, 4194304},  # 重要警告
        "medium": {2097152, 1048576, 524288, 262144, 131072, 65536},  # 次要警告
        "low": {32768, 8192, 4096, 2048, 1024, 512, 256, 128, 64, 16, 8, 4, 2, 1}  # 提示
        }
    def __init__(self, devicetype):
        
        self.devicetype = devicetype    

        self.ERROR_MESSAGE_MAP = {
            2147483648: "SPI Fail - 內部通訊異常",
            1073741824: "EEPROM R/W Fail - 存儲讀寫異常",
            536870912: "Fac Fail - 電網頻率超限",
            268435456: "AFCI Fault - 直流拉弧故障",
            134217728: "Night SPS Fault - 夜間 SPS 異常",
            67108864: "L-PE Fail - 火線對地短路",
            33554432: "Relay Chk Fail - 繼電器自檢異常",
            16777216: "N-PE Fail - N 線對地異常",
            8388608: "ARCFail-HW - 硬件防逆流故障",
            4194304: "Pv Reverse Fault - PV 反接故障",
            2097152: "String OverCurr - 组串电流过流",
            1048576: "LCD Comm Fail - LCD 通訊異常",
            524288: "DCI High - 直流分量高",
            262144: "Isolation Fail - 绝缘阻抗低",
            131072: "Vac Fail - 電網電壓超限",
            65536: "EFan Fail - 外風扇異常",
            32768: "PV Over Voltage - 面板電壓過高",
            8192: "Overtemp - 過溫保護",
            4096: "IFan Fail - 內風扇異常",
            2048: "DC Bus High - 母線電壓高",
            1024: "Ground I Fail - 殘餘電流保護",
            512: "Utility Loss - 電網斷電",
            256: "AC HCT Fail - 交流傳感器故障",
            128: "Relay Dev Fail - 繼電器故障",
            64: "GFCI Fail - 漏電流設備故障",
            16: "DC SPD Fail - 直流防雷失效",
            8: "DC Switch Fail - 直流開關超限",
            4: "Ref 1.5V Fail - 1.5V 基準超限",
            2: "AC HCT Chk Fail - 交流自檢異常",
            1: "GFCI Chk Fail - 漏電流自檢異常",
        }

    def classify_alert_level(self, error_code):
        """根據 `error_code` 分類告警等級"""
        for level, codes in self.ERROR_SEVERITY.items():
            if error_code in codes:
                return {
                    "severe": "嚴重警告",
                    "high": "重要警告",
                    "medium": "次要警告",
                    "low": "提示"
                }[level]
        return "未知等級"  # 預設值
    

    def filter_data(self, data):
        """只保留特定欄位"""
        keep_keys = {
            "collecttime",
            "serialnumber",
            "devicetype",
            "errormessage",
            "warningcode",
            "functionbit"
        }
        return {key: data[key] for key in keep_keys if key in data}

    def get_error_message(self, data):
        """ 解析 `error_message`，將錯誤碼轉換為對應的錯誤訊息，並新增告警資訊 """

        filtered_data = self.filter_data(data)
        error_code = data.get("errormessage", 0)  # 取得錯誤碼 (預設為 0)
        
        matched_errors = []
        for bit, message in self.ERROR_MESSAGE_MAP.items():
            if error_code & bit:  # 使用位元運算判斷錯誤碼是否包含該錯誤
                matched_errors = message
        alert_level = self.classify_alert_level(error_code)
        # ✅ 新增告警欄位（繁體）
        alert_data = {
            "alertlevel":alert_level,  # 告警等級
            "alertstatus": "unresolved",  # 告警狀態
            "errormessage":error_code,
            "devicetype": self.devicetype,  # 設備類型
            "devicename": data.get("device_name", "unknown_device"),  # 設備名稱
            "alertcontent": matched_errors,  # 告警內容 (list of error messages)
            "alerttime": data.get("collecttime"),  # 告警時間
            "alertduration": None,  # 告警時長(h) (future calculation)
            "alertrecoverytime": None,  # 告警恢復時間 (to be updated when resolved)
            "handler": None,  # 處理人 (person responsible)
            "handlingtime": None,  # 處理時間 (when it was handled)
            "relatedworkorder": None,  # 關聯工單 (linked work order)
            "action": None  # 操作 (action taken)
        }

        merged_data = {**filtered_data, **alert_data}
        
        return merged_data  # 回傳完整的合併資料





