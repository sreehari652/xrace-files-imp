/*
  ESP32S3 UWB AT Tag - MPU6050 Integration (Debug v7)
  ─────────────────────────────────────────────────────
  Fix: mpu.begin() rejects GY-87 clone due to WHO_AM_I mismatch.
       MPU6050 is now initialized by writing registers directly
       over I2C — no library ID check, works with all clones.

  Wiring:
    MPU6050 VCC  →  3.3V  (J2 pin 2)
    MPU6050 GND  →  GND   (J2 pin 1)
    MPU6050 SDA  →  GPIO5 (J2 pin 5)
    MPU6050 SCL  →  GPIO6 (J2 pin 6)
    MPU6050 AD0  →  GND   (address 0x68)
*/

// ── User config ───────────────────────────────────────────────────────────────

#define UWB_INDEX     0
#define PAN_INDEX     0
#define TAG
#define UWB_TAG_COUNT 3

#define WIFI_SSID  "ZYBO 4G"
#define WIFI_PASS  "ZYBOpass@2030"
#define LAPTOP_IP  "192.168.29.13"
#define UDP_PORT   4211

#define MPU_ADDR   0x68

// ── Includes ──────────────────────────────────────────────────────────────────

#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <Arduino.h>
#include <WiFi.h>
#include <WiFiUdp.h>

// ── Pin Definitions ───────────────────────────────────────────────────────────

HardwareSerial SERIAL_AT(2);

#define RESET    16
#define IO_RXD2  18
#define IO_TXD2  17

#define I2C_SDA  39
#define I2C_SCL  38
#define IMU_SDA  5
#define IMU_SCL  6

// ── Objects ───────────────────────────────────────────────────────────────────

Adafruit_SSD1306 display(128, 64, &Wire, -1);
TwoWire          I2C_IMU = TwoWire(1);
WiFiUDP          udp;
bool             imuReady = false;

// ── Shared volatile IMU fields ────────────────────────────────────────────────

volatile float    imu_ax      = 0;
volatile float    imu_ay      = 0;
volatile float    imu_az      = 0;
volatile float    imu_gx      = 0;
volatile float    imu_gy      = 0;
volatile float    imu_gz      = 0;
volatile float    imu_heading = 0;
volatile uint32_t imu_ts      = 0;

portMUX_TYPE imuMux = portMUX_INITIALIZER_UNLOCKED;

// ── MPU6050 raw register helpers ──────────────────────────────────────────────

void mpuWrite(uint8_t reg, uint8_t val)
{
    I2C_IMU.beginTransmission(MPU_ADDR);
    I2C_IMU.write(reg);
    I2C_IMU.write(val);
    I2C_IMU.endTransmission();
}

int16_t mpuRead16(uint8_t reg)
{
    I2C_IMU.beginTransmission(MPU_ADDR);
    I2C_IMU.write(reg);
    I2C_IMU.endTransmission(false);
    I2C_IMU.requestFrom((uint8_t)MPU_ADDR, (uint8_t)2);
    int16_t val = (I2C_IMU.read() << 8) | I2C_IMU.read();
    return val;
}

bool mpuInit()
{
    // Read WHO_AM_I
    I2C_IMU.beginTransmission(MPU_ADDR);
    I2C_IMU.write(0x75);
    I2C_IMU.endTransmission(false);
    I2C_IMU.requestFrom((uint8_t)MPU_ADDR, (uint8_t)1);
    uint8_t whoami = I2C_IMU.read();
    Serial.printf("[MPU] WHO_AM_I = 0x%02X\n", whoami);

    // Wake up (clear sleep bit in PWR_MGMT_1)
    mpuWrite(0x6B, 0x00);
    delay(100);

    // Gyro config: ±500 deg/s → register 0x1B, value 0x08
    mpuWrite(0x1B, 0x08);
    // Accel config: ±4g → register 0x1C, value 0x08
    mpuWrite(0x1C, 0x08);
    // DLPF config: 21Hz bandwidth → register 0x1A, value 0x04
    mpuWrite(0x1A, 0x04);
    // Sample rate divider: 9 → 100Hz (1000Hz / (1+9))
    mpuWrite(0x19, 0x09);

    Serial.println("[MPU] Registers written. Reading test sample...");

    // Test read
    int16_t ax = mpuRead16(0x3B);
    int16_t ay = mpuRead16(0x3D);
    int16_t az = mpuRead16(0x3F);
    int16_t gx = mpuRead16(0x43);
    int16_t gy = mpuRead16(0x45);
    int16_t gz = mpuRead16(0x47);

    Serial.printf("[MPU] raw ax=%d ay=%d az=%d gx=%d gy=%d gz=%d\n",
                  ax, ay, az, gx, gy, gz);

    // az should be ~8192 (1g at ±4g scale) when flat, not 0
    return (ax != 0 || ay != 0 || az != 0);
}

// ── Conversion helpers ────────────────────────────────────────────────────────

// ±4g range: 32768 / 4 = 8192 LSB/g,  multiply by 9.81 for m/s²
#define ACCEL_SCALE  (9.81f / 8192.0f)
// ±500 deg/s range: 32768 / 500 = 65.536 LSB/(deg/s)
#define GYRO_SCALE_DEG  (1.0f / 65.536f)
#define GYRO_SCALE_RAD  (GYRO_SCALE_DEG * PI / 180.0f)

// ── IMU Task — Core 1 ─────────────────────────────────────────────────────────

void imuTask(void *pvParameters)
{
    Serial.println("[IMU] Task running on Core 1");

    float    heading  = 0.0f;
    uint32_t lastTime = millis();
    uint32_t logTimer = millis();

    for (;;) {
        // Read all 6 axes in one burst (registers 0x3B–0x48, skip temp)
        I2C_IMU.beginTransmission(MPU_ADDR);
        I2C_IMU.write(0x3B);
        I2C_IMU.endTransmission(false);
        I2C_IMU.requestFrom((uint8_t)MPU_ADDR, (uint8_t)14);

        int16_t raw_ax = (I2C_IMU.read() << 8) | I2C_IMU.read();
        int16_t raw_ay = (I2C_IMU.read() << 8) | I2C_IMU.read();
        int16_t raw_az = (I2C_IMU.read() << 8) | I2C_IMU.read();
        I2C_IMU.read(); I2C_IMU.read();  // skip temp
        int16_t raw_gx = (I2C_IMU.read() << 8) | I2C_IMU.read();
        int16_t raw_gy = (I2C_IMU.read() << 8) | I2C_IMU.read();
        int16_t raw_gz = (I2C_IMU.read() << 8) | I2C_IMU.read();

        float ax = raw_ax * ACCEL_SCALE;
        float ay = raw_ay * ACCEL_SCALE;
        float az = raw_az * ACCEL_SCALE;
        float gx = raw_gx * GYRO_SCALE_RAD;
        float gy = raw_gy * GYRO_SCALE_RAD;
        float gz = raw_gz * GYRO_SCALE_RAD;

        uint32_t now = millis();
        float    dt  = (now - lastTime) / 1000.0f;
        lastTime     = now;

        heading += (gz * 180.0f / PI) * dt;
        if (heading >= 360.0f) heading -= 360.0f;
        if (heading <  0.0f)   heading += 360.0f;

        portENTER_CRITICAL(&imuMux);
        imu_ax      = ax;
        imu_ay      = ay;
        imu_az      = az;
        imu_gx      = gx;
        imu_gy      = gy;
        imu_gz      = gz;
        imu_heading = heading;
        imu_ts      = now;
        portEXIT_CRITICAL(&imuMux);

        // Print every 2 seconds
        if (millis() - logTimer > 2000) {
            Serial.printf("AX=%.2f AY=%.2f AZ=%.2f\n", ax, ay, az);
            Serial.printf("GX=%.3f GY=%.3f GZ=%.3f\n", gx, gy, gz);
            Serial.printf("[IMU] hdg=%.2f  ts=%u\n", heading, now);
            Serial.flush();
            logTimer = millis();
        }

        vTaskDelay(pdMS_TO_TICKS(10));  // 100Hz
    }
}

// ── Send IMU snapshot via UDP ─────────────────────────────────────────────────

void sendIMU_UDP()
{
    portENTER_CRITICAL(&imuMux);
    float    d_heading = imu_heading;
    float    d_gz      = imu_gz;
    float    d_ax      = imu_ax;
    float    d_ay      = imu_ay;
    float    d_az      = imu_az;
    float    d_gx      = imu_gx;
    float    d_gy      = imu_gy;
    uint32_t d_ts      = imu_ts;
    portEXIT_CRITICAL(&imuMux);

    String packet = "T" + String(UWB_INDEX) + ",IMU,"
                  + String(d_heading, 2) + ","
                  + String(d_gz,      4) + ","
                  + String(d_ax,      3) + ","
                  + String(d_ay,      3) + ","
                  + String(d_az,      3) + ","
                  + String(d_gx,      4) + ","
                  + String(d_gy,      4) + ","
                  + String(d_ts);

    udp.beginPacket(LAPTOP_IP, UDP_PORT);
    udp.print(packet);
    udp.endPacket();

    Serial.println("IMU UDP: " + packet);
}

// ── Setup ─────────────────────────────────────────────────────────────────────

void setup()
{
    pinMode(RESET, OUTPUT);
    digitalWrite(RESET, HIGH);

    Serial.begin(115200);
    Serial.println(F("\nHello! ESP32-S3 AT + MPU6050 v7"));

    SERIAL_AT.begin(115200, SERIAL_8N1, IO_RXD2, IO_TXD2);
    SERIAL_AT.println("AT");

    Wire.begin(I2C_SDA, I2C_SCL);
    delay(1000);

    if (!display.begin(SSD1306_SWITCHCAPVCC, 0x3C)) {
        Serial.println(F("SSD1306 failed"));
        for (;;);
    }
    display.clearDisplay();
    logoshow();

    // WiFi
    WiFi.begin(WIFI_SSID, WIFI_PASS);
    display.clearDisplay();
    display.setTextSize(1);
    display.setTextColor(SSD1306_WHITE);
    display.setCursor(0, 0);
    display.println("Connecting WiFi...");
    display.display();

    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println("\n[WiFi] Connected: " + WiFi.localIP().toString());
    udp.begin(UDP_PORT);

    display.clearDisplay();
    display.setCursor(0, 0);
    display.println("WiFi OK");
    display.setCursor(0, 16);
    display.println(WiFi.localIP());
    display.display();
    delay(500);

    // MPU6050 direct register init
    Serial.println("[Setup] I2C bus 1 on GPIO5/GPIO6...");
    I2C_IMU.begin(IMU_SDA, IMU_SCL, 400000);
    delay(200);

    if (mpuInit()) {
        Serial.println("[Setup] MPU6050 ready");
        imuReady = true;
        xTaskCreatePinnedToCore(imuTask, "IMU_Task", 8192, NULL, 2, NULL, 1);
        Serial.println("[Setup] IMU task created on Core 1");
    } else {
        Serial.println("[Setup] MPU6050 returned all zeros — check wiring");
    }

    // UWB AT init
    Serial.println("[Setup] UWB init...");
    sendData("AT?",         2000, 1);
    sendData("AT+RESTORE",  5000, 1);
    sendData(config_cmd(),  2000, 1);
    sendData(cap_cmd(),     2000, 1);
    sendData("AT+SETRPT=1", 2000, 1);
    sendData(Pan_cmd(),     2000, 1);
    sendData("AT+SAVE",     2000, 1);
    sendData("AT+RESTART",  2000, 1);
    Serial.println("[Setup] Done.");
}

// ── Main loop — UWB on Core 0 ─────────────────────────────────────────────────

String response = "";

void loop()
{
    while (SERIAL_AT.available() > 0) {
        char c = SERIAL_AT.read();

        if (c == '\r') continue;
        else if (c == '\n') {
            Serial.println(response);

            if (response.startsWith("AT+RANGE") && imuReady) {
                sendIMU_UDP();
            }

            response = "";
        }
        else {
            response += c;
        }
    }
}

// ── OLED logo ─────────────────────────────────────────────────────────────────

void logoshow(void)
{
    display.clearDisplay();
    display.setTextSize(1);
    display.setTextColor(SSD1306_WHITE);
    display.setCursor(0, 0);
    display.println(F("MaUWB DW3000"));
    display.setCursor(0, 20);
    display.setTextSize(2);
    display.println("T" + String(UWB_INDEX) + "   6.8M");
    display.setCursor(0, 40);
    display.println("Total: " + String(UWB_TAG_COUNT));
    display.display();
    delay(2000);
}

// ── AT helpers ────────────────────────────────────────────────────────────────

String sendData(String command, const int timeout, boolean debug)
{
    String response = "";
    Serial.println(command);
    SERIAL_AT.println(command);
    long int time = millis();
    while ((time + timeout) > millis()) {
        while (SERIAL_AT.available()) {
            char c = SERIAL_AT.read();
            response += c;
        }
    }
    if (debug) Serial.println(response);
    return response;
}

String config_cmd()
{
    return "AT+SETCFG=" + String(UWB_INDEX) + ",0,1,0";
}

String cap_cmd()
{
    return "AT+SETCAP=" + String(UWB_TAG_COUNT) + ",10,1";
}

String Pan_cmd()
{
    return "AT+SETPAN=" + String(PAN_INDEX);
}
