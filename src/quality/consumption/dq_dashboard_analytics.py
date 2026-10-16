"""DQ oficial da metodologia final B; referências A preservadas em sensibilidade."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]))
from src.quality.consumption.dq_metodologia_final import main
if __name__=='__main__':raise SystemExit(main())
