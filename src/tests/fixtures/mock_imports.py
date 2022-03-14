import mock
import sys
sys.modules['enroute'] = mock.MagicMock()
sys.modules['runon'] = mock.MagicMock()
sys.modules['Interfaces'] = mock.MagicMock()
sys.modules['hostaddr'] = mock.MagicMock()
sys.modules['checksum'] = mock.MagicMock()
