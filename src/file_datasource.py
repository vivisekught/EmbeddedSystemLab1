from datetime import datetime

from domain.accelerometer import Accelerometer
from domain.aggregated_data import AggregatedData
from domain.gps import Gps
from domain.parking import Parking
from csv import reader


class FileDatasource:

    def __init__(
            self,
            accelerometer_filename: str,
            gps_filename: str,
            parking_filename: str
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename

    def read_accelerometer(self) -> AggregatedData:
        try:
            # Accelerometer
            accelerometer_data = self._read_accelerometer_data()
            x, y, z = map(float, accelerometer_data)

            # Gps
            gps_data = self._read_gps_data()
            lat, lon = map(float, gps_data)
            # return AggregatedData
            return AggregatedData(
                Accelerometer(x=x, y=y, z=z),
                Gps(latitude=lat, longitude=lon),
                datetime.now(),
            )
        except Exception as e:
            print(f"Exeption {e}")

    def read_parking(self) -> Parking:
        try:
            parking_data = self._read_parking_data()
            count, lat, lon = map(float, parking_data)
            return Parking(empty_count=count, gps=Gps(lat, lon))
        except Exception as e:
            print(f"Exeption {e}")

    def startReading(self, *args, **kwargs):
        self._open_file()
        self._skip_first_row()

    def stopReading(self, *args, **kwargs):
        self.accelerometer_csv_file.close()
        self.gps_csv_file.close()
        self.parking_csv_file.close()

    def _read_accelerometer_data(self):
        return next(reader(self.accelerometer_csv_file))

    def _read_gps_data(self):
        return next(reader(self.gps_csv_file))

    def _read_parking_data(self):
        return next(reader(self.parking_csv_file))

    def _open_file(self):
        self.accelerometer_csv_file = open(self.accelerometer_filename, 'r')
        self.gps_csv_file = open(self.gps_filename, 'r')
        self.parking_csv_file = open(self.parking_filename, 'r')

    def _skip_first_row(self):
        next(self.accelerometer_csv_file)
        next(self.gps_csv_file)
        next(self.parking_csv_file)