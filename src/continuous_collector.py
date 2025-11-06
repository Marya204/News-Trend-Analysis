"""
Collecteur continu - Exécution automatique toutes les X heures
Maximise l'accumulation de données nouvelles
Version corrigée pour Windows avec gestion des chemins
"""

import schedule
import time
from datetime import datetime
import subprocess
import sys
import json
import os
import logging
import re

# --------------------------
# Configuration UTF-8 pour Windows
# --------------------------
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# --------------------------
# Configuration des chemins
# --------------------------
# Obtenir le dossier où se trouve ce script (src/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Dossier parent (News_Trend_Analysis/)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# Chemins absolus
TRACKING_DIR = os.path.join(PROJECT_ROOT, "data", "tracking")
os.makedirs(TRACKING_DIR, exist_ok=True)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(TRACKING_DIR, 'collector_log.txt'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --------------------------
# Configuration
# --------------------------
COLLECT_INTERVAL_HOURS = 6  # Intervalle entre chaque collecte (6h recommandé)

# --------------------------
# Statistiques de session
# --------------------------
class SessionStats:
    def __init__(self):
        self.stats_file = os.path.join(TRACKING_DIR, "session_stats.json")
        self.load_stats()
    
    def load_stats(self):
        if os.path.exists(self.stats_file):
            try:
                with open(self.stats_file, 'r', encoding='utf-8') as f:
                    self.stats = json.load(f)
            except:
                self.stats = self._default_stats()
        else:
            self.stats = self._default_stats()
    
    def _default_stats(self):
        return {
            'total_runs': 0,
            'total_collected': 0,
            'started_at': datetime.now().isoformat(),
            'runs': []
        }
    
    def add_run(self, new_data_count: int, duration: float):
        self.stats['total_runs'] += 1
        self.stats['total_collected'] += new_data_count
        self.stats['runs'].append({
            'timestamp': datetime.now().isoformat(),
            'new_data': new_data_count,
            'duration_seconds': duration
        })
        
        # Garder seulement les 100 dernières exécutions
        self.stats['runs'] = self.stats['runs'][-100:]
        
        try:
            with open(self.stats_file, 'w', encoding='utf-8') as f:
                json.dump(self.stats, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erreur sauvegarde stats: {e}")
    
    def get_summary(self):
        if not self.stats['runs']:
            return "Aucune collecte effectuee"
        
        recent_runs = self.stats['runs'][-10:]
        avg_new_data = sum(r['new_data'] for r in recent_runs) / len(recent_runs)
        total_time = sum(r['duration_seconds'] for r in recent_runs)
        
        return f"""
STATISTIQUES DE SESSION
{'='*60}
Executions totales: {self.stats['total_runs']}
Donnees collectees: {self.stats['total_collected']:,}
Moyenne (10 dernieres): {avg_new_data:.0f} nouvelles donnees
Temps moyen: {total_time/len(recent_runs):.1f}s
Demarre: {self.stats['started_at']}
{'='*60}
"""

session_stats = SessionStats()

# --------------------------
# Fonction de collecte
# --------------------------
def run_collection():
    """Exécute le script de collecte"""
    logger.info("\n" + "="*80)
    logger.info("DEMARRAGE DE LA COLLECTE")
    logger.info("="*80)
    
    start_time = time.time()
    
    try:
        # Trouver le chemin du script de collecte
        collector_script = os.path.join(SCRIPT_DIR, "collect_data_tracking.py")
        
        # Vérifier que le fichier existe
        if not os.path.exists(collector_script):
            logger.error(f"ERREUR: Fichier introuvable: {collector_script}")
            logger.error("Assurez-vous que collect_data_tracking.py est dans le meme dossier")
            logger.error(f"Dossier actuel: {SCRIPT_DIR}")
            try:
                logger.error(f"Fichiers disponibles: {os.listdir(SCRIPT_DIR)}")
            except:
                pass
            return
        
        logger.info(f"Execution: {collector_script}")
        
        # Exécuter le script de collecte depuis le bon dossier
        result = subprocess.run(
            [sys.executable, collector_script],
            capture_output=True,
            text=True,
            cwd=SCRIPT_DIR,  # Important: définir le working directory
            timeout=1800,  # 30 minutes max
            encoding='utf-8',
            errors='replace'
        )
        
        duration = time.time() - start_time
        
        # Parser la sortie pour extraire le nombre de nouvelles données
        new_data_count = 0
        
        # Chercher "Nouveaux: XXX elements"
        for line in result.stdout.split('\n'):
            # Pattern: "Nouveaux: 123 elements" ou "Total: 123"
            if "Nouveaux:" in line:
                try:
                    numbers = re.findall(r'Nouveaux:\s*(\d+)', line)
                    if numbers:
                        new_data_count = int(numbers[0])
                        break
                except:
                    pass
            elif "Total hashes:" in line and "+(" in line:
                # Pattern: "Total hashes: 1,234 (+123)"
                try:
                    numbers = re.findall(r'\+(\d+)', line)
                    if numbers:
                        new_data_count = int(numbers[0])
                        break
                except:
                    pass
        
        # Enregistrer les stats
        session_stats.add_run(new_data_count, duration)
        
        logger.info(f"\nCollecte terminee: {new_data_count} nouvelles donnees en {duration:.1f}s")
        
        # Afficher les dernières lignes de sortie (résumé)
        output_lines = result.stdout.split('\n')
        logger.info("\nResultat de la collecte:")
        logger.info("-" * 80)
        
        # Afficher les 30 dernières lignes non vides
        relevant_lines = [line for line in output_lines if line.strip()][-30:]
        for line in relevant_lines:
            logger.info(line)
        
        if result.returncode != 0:
            logger.error(f"\nErreurs detectees (code: {result.returncode}):")
            stderr_lines = [line for line in result.stderr.split('\n') if line.strip()]
            for line in stderr_lines[-20:]:  # 20 dernières lignes d'erreur
                logger.error(line)
        
        # Afficher le résumé de session
        logger.info("\n" + session_stats.get_summary())
        
    except subprocess.TimeoutExpired:
        logger.error("ERREUR: La collecte a pris plus de 30 minutes")
        duration = time.time() - start_time
        session_stats.add_run(0, duration)
    except FileNotFoundError as e:
        logger.error(f"ERREUR: Fichier non trouve: {e}")
    except Exception as e:
        logger.error(f"ERREUR lors de la collecte: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("="*80 + "\n")

# --------------------------
# Planification
# --------------------------
def schedule_collections():
    """Configure le planning de collecte"""
    
    logger.info("\n" + "="*70)
    logger.info("         COLLECTEUR CONTINU DE DONNEES - DEMARRE")
    logger.info("="*70 + "\n")
    
    logger.info(f"Intervalle configure: toutes les {COLLECT_INTERVAL_HOURS} heures")
    logger.info(f"Dossier de travail: {SCRIPT_DIR}")
    logger.info(f"Dossier projet: {PROJECT_ROOT}")
    logger.info(f"Logs: {os.path.join(TRACKING_DIR, 'collector_log.txt')}")
    logger.info(f"Stats: {os.path.join(TRACKING_DIR, 'session_stats.json')}\n")
    
    # Exécuter immédiatement la première collecte
    logger.info("Premiere collecte (immediate)...\n")
    run_collection()
    
    # Planifier les collectes suivantes
    schedule.every(COLLECT_INTERVAL_HOURS).hours.do(run_collection)
    
    next_run = datetime.now().replace(microsecond=0) + timedelta(hours=COLLECT_INTERVAL_HOURS)
    logger.info(f"\nProchaine collecte: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("Le collecteur tourne en continu. Appuyez sur Ctrl+C pour arreter.\n")
    
    # Boucle infinie
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Vérifier toutes les minutes
    except KeyboardInterrupt:
        logger.info("\n\nArret du collecteur...")
        logger.info(session_stats.get_summary())
        logger.info("Au revoir!\n")

# --------------------------
# Commandes utiles
# --------------------------
def show_stats():
    """Affiche les statistiques actuelles"""
    print(session_stats.get_summary())
    
    if session_stats.stats['runs']:
        print("\nDernieres collectes:")
        print("-" * 60)
        for run in session_stats.stats['runs'][-5:]:
            timestamp = datetime.fromisoformat(run['timestamp']).strftime('%Y-%m-%d %H:%M')
            print(f"  {timestamp} -> {run['new_data']:4d} nouvelles donnees ({run['duration_seconds']:.1f}s)")

def reset_stats():
    """Réinitialise les statistiques"""
    if os.path.exists(session_stats.stats_file):
        os.remove(session_stats.stats_file)
    logger.info("Statistiques reinitialisees")

# --------------------------
# Point d'entrée
# --------------------------
if __name__ == "__main__":
    import argparse
    from datetime import timedelta
    
    parser = argparse.ArgumentParser(description="Collecteur continu de donnees")
    parser.add_argument('--stats', action='store_true', help='Afficher les statistiques')
    parser.add_argument('--reset', action='store_true', help='Reinitialiser les statistiques')
    parser.add_argument('--once', action='store_true', help='Executer une seule collecte')
    parser.add_argument('--interval', type=int, default=6, help='Intervalle en heures (defaut: 6)')
    
    args = parser.parse_args()
    
    if args.stats:
        show_stats()
    elif args.reset:
        reset_stats()
    elif args.once:
        run_collection()
    else:
        COLLECT_INTERVAL_HOURS = args.interval
        schedule_collections()