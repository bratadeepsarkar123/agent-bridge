import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import supervisor


class SupervisorTests(unittest.TestCase):
    def test_status_does_not_restart_without_proof_of_crash(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / 'state.json'
            with patch.object(supervisor, 'STATE', state):
                self.assertEqual('stopped', supervisor.inspect_broker())
                state.write_text('bad json', encoding='utf-8')
                self.assertEqual('invalid', supervisor.inspect_broker())
                state.write_text(json.dumps({'pid': 12345}), encoding='utf-8')
                with patch.object(supervisor, '_process_is_alive', return_value=True):
                    self.assertEqual('running', supervisor.inspect_broker())
                with patch.object(supervisor, '_process_is_alive', return_value=False):
                    self.assertEqual('crashed', supervisor.inspect_broker())


if __name__ == '__main__':
    unittest.main()
