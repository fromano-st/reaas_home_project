"""
Data schema definitions for IoT events.
"""
from pydantic import BaseModel, Field, ConfigDict, field_serializer
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum


class DeviceType(str, Enum):
    """Supported device types for IoT events."""

    TEMPERATURE_SENSOR = "temperature_sensor"
    HUMIDITY_SENSOR = "humidity_sensor"
    PRESSURE_SENSOR = "pressure_sensor"
    MOTION_DETECTOR = "motion_detector"
    LIGHT_SENSOR = "light_sensor"
    SMART_THERMOSTAT = "smart_thermostat"
    DOOR_SENSOR = "door_sensor"
    WINDOW_SENSOR = "window_sensor"


class IoTEvent(BaseModel):
    """
    Schema for IoT event data.

    Required fields as per specification:
    - event_duration: Duration of the event in seconds
    - device_type: Type of the device generating the event

    Additional fields for realistic IoT simulation:
    - device_id: Unique identifier for the device
    - timestamp: When the event occurred
    - location: Physical location of the device
    - value: Sensor reading or measurement value
    - unit: Unit of measurement
    - battery_level: Battery percentage (0-100)
    - signal_strength: Signal strength in dBm
    - metadata: Additional device-specific data
    """

    # Required fields
    event_duration: float = Field(
        ..., description="Duration of the event in seconds", ge=0.1, le=3600.0
    )
    device_type: DeviceType = Field(..., description="Type of the device")

    # Core event fields
    device_id: str = Field(..., description="Unique device identifier")
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Event timestamp"
    )
    location: str = Field(..., description="Device location")

    # Sensor data
    value: float = Field(..., description="Sensor measurement value")
    unit: str = Field(..., description="Unit of measurement")

    # Device status
    battery_level: Optional[int] = Field(
        None, description="Battery level percentage", ge=0, le=100
    )
    signal_strength: Optional[int] = Field(
        None, description="Signal strength in dBm", ge=-120, le=-30
    )

    # Additional metadata
    metadata: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Additional device metadata"
    )

    model_config = ConfigDict(use_enum_values=True)

    @field_serializer("timestamp")
    def serialize_datetime(self, value: datetime) -> str:
        return value.isoformat()


# Schema documentation for reference
SCHEMA_DOCUMENTATION = {
    "name": "IoT Event Schema",
    "version": "1.0.0",
    "description": "Schema for IoT device events in the streaming ETL pipeline",
    "required_fields": [
        {
            "name": "event_duration",
            "type": "float",
            "description": "Duration of the event in seconds",
            "constraints": "0.1 <= value <= 3600.0",
        },
        {
            "name": "device_type",
            "type": "string",
            "description": "Type of the device generating the event",
            "allowed_values": [e.value for e in DeviceType],
        },
    ],
    "optional_fields": [
        {
            "name": "device_id",
            "type": "string",
            "description": "Unique identifier for the device",
        },
        {
            "name": "timestamp",
            "type": "datetime",
            "description": "ISO format timestamp of when the event occurred",
        },
        {
            "name": "location",
            "type": "string",
            "description": "Physical location of the device",
        },
        {
            "name": "value",
            "type": "float",
            "description": "Sensor reading or measurement value",
        },
        {
            "name": "unit",
            "type": "string",
            "description": "Unit of measurement for the value",
        },
        {
            "name": "battery_level",
            "type": "integer",
            "description": "Battery level percentage (0-100)",
        },
        {
            "name": "signal_strength",
            "type": "integer",
            "description": "Signal strength in dBm (-120 to -30)",
        },
        {
            "name": "metadata",
            "type": "object",
            "description": "Additional device-specific metadata",
        },
    ],
    "example": {
        "event_duration": 2.5,
        "device_type": "temperature_sensor",
        "device_id": "temp_001_living_room",
        "timestamp": "2025-07-26T19:19:13.123456",
        "location": "Living Room",
        "value": 22.5,
        "unit": "°C",
        "battery_level": 85,
        "signal_strength": -65,
        "metadata": {"firmware_version": "1.2.3", "calibration_date": "2025-01-15"},
    },
}
