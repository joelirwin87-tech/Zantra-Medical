import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from agents import appointments
from connector import HaloFHIRClient, HaloSQLClient


class AppointmentAgentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sql_client = HaloSQLClient()
        self.fhir_client = HaloFHIRClient()
        self.fhir_client.register_patient("patient-1")

        self.sql_patcher = patch("agents.appointments.SQL_CLIENT", self.sql_client)
        self.fhir_patcher = patch("agents.appointments.FHIR_CLIENT", self.fhir_client)
        self.notify_patcher = patch("agents.appointments.send_notification")

        self.sql_patcher.start()
        self.fhir_patcher.start()
        self.mock_notify = self.notify_patcher.start()

    def tearDown(self) -> None:
        self.notify_patcher.stop()
        self.fhir_patcher.stop()
        self.sql_patcher.stop()

    def test_book_appointment_success(self) -> None:
        appointment_time = datetime.now(timezone.utc) + timedelta(days=1)

        appointment = appointments.book_appointment(
            "patient-1", "provider-1", appointment_time
        )

        self.assertEqual(appointment["patient_id"], "patient-1")
        self.assertEqual(appointment["provider_id"], "provider-1")
        self.assertEqual(appointment["datetime"], appointment_time)
        self.mock_notify.assert_called_once()

    def test_book_appointment_rejects_unavailable_slot(self) -> None:
        appointment_time = datetime.now(timezone.utc) + timedelta(days=1)
        appointments.book_appointment("patient-1", "provider-1", appointment_time)
        self.mock_notify.reset_mock()

        with self.assertRaises(ValueError):
            appointments.book_appointment("patient-1", "provider-1", appointment_time)

        self.mock_notify.assert_not_called()

    def test_book_appointment_requires_registered_patient(self) -> None:
        appointment_time = datetime.now(timezone.utc) + timedelta(days=1)

        with self.assertRaises(ValueError):
            appointments.book_appointment("unknown", "provider-1", appointment_time)

    def test_cancel_appointment(self) -> None:
        appointment_time = datetime.now(timezone.utc) + timedelta(days=1)
        appointment = appointments.book_appointment(
            "patient-1", "provider-1", appointment_time
        )
        appointment_id = appointment["appointment_id"]

        appointments.cancel_appointment(appointment_id)

        schedule = appointments.get_patient_schedule("patient-1")
        self.assertEqual(schedule, [])

    def test_cancel_nonexistent_appointment_raises_error(self) -> None:
        with self.assertRaises(ValueError):
            appointments.cancel_appointment("999")

    def test_get_patient_schedule_sorted(self) -> None:
        early = datetime.now(timezone.utc) + timedelta(days=1)
        late = early + timedelta(hours=2)

        appointments.book_appointment("patient-1", "provider-1", late)
        appointments.book_appointment("patient-1", "provider-2", early)

        schedule = appointments.get_patient_schedule("patient-1")
        self.assertEqual(len(schedule), 2)
        self.assertEqual(schedule[0]["datetime"], early)
        self.assertEqual(schedule[1]["datetime"], late)


if __name__ == "__main__":
    unittest.main()
