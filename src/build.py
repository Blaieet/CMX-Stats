import polars as pl
from jinja2 import Environment, FileSystemLoader
import os
import shutil

# Configuration
CSV_PLAYERS = "LaMasia 25_26 - Jugadors.csv"
CSV_WEEKS = "Còpia de LaMasia 25_26 - Jornades.csv"
OUTPUT_DIR = "docs"
TEMPLATES_DIR = "templates"
ASSETS_DIR = "assets"

def load_data():
    # Load Players Data
    try:
        df_players = pl.read_csv(CSV_PLAYERS, null_values=["#DIV/0!"])
        # Filter out empty rows if any (based on 'Jugador' column)
        df_players = df_players.filter(pl.col("Jugador").is_not_null())
        
        # Cast numeric columns safely to handle potential garbage rows or formatting issues
        # We assume 'Partits' must be a valid number for a valid player row
        numeric_cols_int = ["Partits", "Gols", "Assistències", "Grogues", "Vermelles", "Expulsions",
                            "Normal", "Segon Pal", "Penalti", "D. Penalti", "Tir Lliure (falta)"]
        for col in numeric_cols_int:
            if col in df_players.columns:
                df_players = df_players.with_columns(pl.col(col).cast(pl.Int64, strict=False))
        
        # Handle Float columns that might have commas
        numeric_cols_float = ["Win Rate", "Loss Rate", "Draw Rate", "Gols x partit", 
                              "Partits per gol", "Assitències x partit", "Grogues_Ratio",
                              "Minuts sense gols (porter)", "Gols x partit en contra", 
                              "Gols x minut en contra"]
        for col in numeric_cols_float:
            if col in df_players.columns:
                # Replace , with . if string, then cast
                df_players = df_players.with_columns(
                    pl.col(col).cast(pl.String).str.replace(",", ".").cast(pl.Float64, strict=False)
                )

        # Special handling: "Minuts sense gols (porter)" should be int for logic, but comes as float
        if "Minuts sense gols (porter)" in df_players.columns:
             df_players = df_players.with_columns(
                 pl.col("Minuts sense gols (porter)").fill_null(0).cast(pl.Int64, strict=False)
             )

        # Filter out rows where 'Partits' is null (implies invalid row or garbage)
        df_players = df_players.filter(pl.col("Partits").is_not_null())
        
        # Also clean up float columns if needed
        # Win Rate etc are Floats
        
        print(f"Loaded {len(df_players)} players.")
    except Exception as e:
        print(f"Error loading players CSV: {e}")
        df_players = pl.DataFrame()

    # Load Weeks Data
    try:
        df_weeks = pl.read_csv(CSV_WEEKS, null_values=["#DIV/0!"])
        # Filter valid weeks (Jornada must exist)
        df_weeks = df_weeks.filter(pl.col("Jornada").is_not_null())
        print(f"Loaded {len(df_weeks)} weeks.")
    except Exception as e:
        print(f"Error loading weeks CSV: {e}")
        df_weeks = pl.DataFrame()

    # Rounding logic helper
    def round_df(df):
        # Select all float columns and round them to 2 decimal places
        return df.with_columns(
            [pl.col(c).round(2) for c in df.columns if df[c].dtype == pl.Float64]
        )

    df_players = round_df(df_players)
    df_weeks = round_df(df_weeks)

    return df_players, df_weeks

def calculate_stats(df_weeks):
    total_matches = len(df_weeks)
    
    # Clean and cast columns if necessary
    # Example: 'Punts' count, Wins, etc.
    # Resultat column: 'Victòria', 'Derrota', 'Empat'
    
    wins = df_weeks.filter(pl.col("Resultat") == "Victòria").height
    draws = df_weeks.filter(pl.col("Resultat") == "Empat").height
    losses = df_weeks.filter(pl.col("Resultat") == "Derrota").height
    
    # Calculate total goals
    # Handle possible nulls or non-numeric by filling 0
    total_goals_for = df_weeks["Marcador Local"].sum() + df_weeks["Marcador Visitant"].sum()
    
    # Actually, we need to know if we are Local or Visitant to know "Goals For"
    # The 'Posició' column tells us 'Local' or 'Visitant'
    
    goals_for = 0
    goals_against = 0
    
    for row in df_weeks.iter_rows(named=True):
        local_goals = row['Marcador Local'] if row['Marcador Local'] is not None else 0
        visitor_goals = row['Marcador Visitant'] if row['Marcador Visitant'] is not None else 0
        
        if row['Posició'] == 'Local':
            goals_for += local_goals
            goals_against += visitor_goals
        elif row['Posició'] == 'Visitant':
            goals_for += visitor_goals
            goals_against += local_goals

    return {
        "total_matches": total_matches,
        "total_wins": wins,
        "total_draws": draws,
        "total_losses": losses,
        "total_goals_for": goals_for,
        "total_goals_against": goals_against
    }

def clean_output_dir():
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)
    
    # Copy assets
    if os.path.exists(ASSETS_DIR):
        shutil.copytree(ASSETS_DIR, os.path.join(OUTPUT_DIR, ASSETS_DIR))

def render_pages(df_players, df_weeks, stats):
    env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
    
    # Prepare data for templates
    # Convert polars DF to list of dicts for Jinja
    players_data = df_players.to_dicts()
    weeks_data = df_weeks.to_dicts()
    
    # Recent matches (last 5 reversed)
    recent_matches = weeks_data[-5:][::-1] if len(weeks_data) > 0 else []

    weekly_stats = stats  # Renaming for clarity if needed

    # Filter Goalkeepers
    # Columns: Jugador, Minuts, Gols en contra, Gols x partit en contra, Gols x minut en contra, Minuts sense gols (porter)
    # The user identified specific GKs, but we can also check if 'Minuts' is populated.
    # Specific GKs:
    gk_names = ["SANCHEZ LAYA, PAU", "GISBERT PEREZ, ORIOL", "RAS JIMENEZ, BLAI"]
    
    # Filter players who are in the gk_names list
    df_goalkeepers = df_players.filter(pl.col("Jugador").is_in(gk_names))
    
    # If using Polars to extract specific columns might be cleaner for the template, 
    # but passing the whole dict is fine as keys exist.
    goalkeepers_data = df_goalkeepers.to_dicts()

    # Prepare Data for Charts
    # We need: labels (Rival), points, goals_for, goals_against
    # Important: Ensure the order matches the 'Jornada' order.
    # We sort by Jornada just in case.
    df_sorted = df_weeks.sort("Jornada")
    
    chart_labels = []
    chart_points = []
    chart_goals_for = []
    chart_goals_against = []
    chart_results = []
    
    running_points = 0
    running_goals_for = 0
    running_goals_against = 0
    
    for row in df_sorted.iter_rows(named=True):
        chart_labels.append(row['vs.'])
        chart_results.append(row['Resultat'])
        
        # Points
        points = 0
        if row['Resultat'] == 'Victòria':
            points = 3
        elif row['Resultat'] == 'Empat':
            points = 1
        
        running_points += points
        chart_points.append(running_points)
            
        # Goals
        local_goals = row['Marcador Local'] if row['Marcador Local'] is not None else 0
        visitor_goals = row['Marcador Visitant'] if row['Marcador Visitant'] is not None else 0
        
        match_gf = 0
        match_ga = 0
        
        if row['Posició'] == 'Local':
            match_gf = local_goals
            match_ga = visitor_goals
        elif row['Posició'] == 'Visitant':
            match_gf = visitor_goals
            match_ga = local_goals
            
        running_goals_for += match_gf
        running_goals_against += match_ga
        
        chart_goals_for.append(running_goals_for)
        chart_goals_against.append(running_goals_against)

    chart_data = {
        "labels": chart_labels,
        "points": chart_points,
        "goals_for": chart_goals_for,
        "goals_against": chart_goals_against,
        "results": chart_results
    }
    
    # Helper to create safe filenames
    import re
    import unicodedata
    def slugify(text):
        text = str(text).lower().strip()
        # Normalize unicode to decompose accents (e.g. 'à' -> 'a' + '`') and then remove combining characters
        text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[\s_-]+', '-', text)
        return text

    # Add URL to player dicts AND output back to DataFrame for leaderboard usage
    urls = []
    for p in players_data:
        p_slug = slugify(p['Jugador'])
        p['filename'] = f"player_{p_slug}.html"
        p['url'] = p['filename']
        urls.append(p['filename'])
        
        # Check for image
        # We check for png, jpg, jpeg
        image_path = None
        for ext in ['.png', '.jpg', '.jpeg', '.webp']:
            potential_path = os.path.join(ASSETS_DIR, "players", f"{p_slug}{ext}")
            if os.path.exists(potential_path):
                # We need the path relative to the docs output
                # But since we copy ASSETS_DIR to docs/ASSETS_DIR, key is just assets/players/...
                p['image_path'] = f"assets/players/{p_slug}{ext}"
                break
        
        if 'image_path' not in p:
            p['image_path'] = None # or a default placeholder if desired
        
    # Add 'url' column to df_players so we can access it in get_top_player
    # We assume df_players order matches players_data order (it should, as to_dicts preserves order)
    df_players = df_players.with_columns(pl.Series("url", urls))

    # helper to get top player
    def get_top_player(df, col, label, suffix=""):
        # Filter nulls first
        valid_df = df.filter(pl.col(col).is_not_null())
        if valid_df.height == 0:
            return None
        
        # Sort descending
        top_row = valid_df.sort(col, descending=True).row(0, named=True)
        return {
            "name": top_row['Jugador'],
            "value": f"{top_row[col]}{suffix}",
            "label": label,
            "url": top_row['url']
        }
    
    # Leaderboard Logic
    leaderboard = []
    
    # 1. Max Scorer
    top_scorer = get_top_player(df_players, "Gols", "Màxim Golejador")
    if top_scorer: leaderboard.append(top_scorer)
    
    # 2. Max Scorer per Game
    top_scorer_pg = get_top_player(df_players, "Gols x partit", "Gols / Partit")
    if top_scorer_pg: leaderboard.append(top_scorer_pg)
    
    # 3. Max Assistant
    top_assistant = get_top_player(df_players, "Assistències", "Màxim Assistent")
    if top_assistant: leaderboard.append(top_assistant)
    
    # 4. Max Assistant per Game
    top_assistant_pg = get_top_player(df_players, "Assitències x partit", "Ass. / Partit")
    if top_assistant_pg: leaderboard.append(top_assistant_pg)
    
    # 5. Best Goalkeeper (Minuts sense gol)
    top_gk = get_top_player(df_players, "Minuts sense gols (porter)", "Millor Porter (Minuts imbatut)", suffix="'")
    if top_gk: leaderboard.append(top_gk)
    
    # 6. Major Win Rate
    # Win Rate is 0-1 in CSV, so we need to multiply by 100 and add %
    # We can't use the generic function easily for the value transformation unless we modify it or do it manually.
    valid_wr = df_players.filter(pl.col("Win Rate").is_not_null())
    if valid_wr.height > 0:
        top_wr_row = valid_wr.sort("Win Rate", descending=True).row(0, named=True)
        # Assuming Win Rate is like 0.75, display as 75.0%
        # Using built-in rounding or format
        val = top_wr_row['Win Rate']
        if isinstance(val, (int, float)):
            val_str = f"{val * 100:.2f}%"
        else:
            val_str = str(val)
            
        leaderboard.append({
            "name": top_wr_row['Jugador'],
            "value": val_str,
            "label": "Major % Victòria",
            "url": top_wr_row['url']
        })

    # 7. Max Yellow Cards Ratio (Grogues / Partit)
    # We need to calculate this ratio safely
    df_players = df_players.with_columns(
        (pl.col("Grogues") / pl.col("Partits")).fill_nan(0).alias("Grogues_Ratio")
    )
    
    top_yellow = get_top_player(df_players, "Grogues_Ratio", "Major % Grogues/Partit")
    if top_yellow and float(top_yellow['value']) > 0:
        # Reformat value to show 2 decimals
        val = float(top_yellow['value'])
        top_yellow['value'] = f"{val:.2f}"
        leaderboard.append(top_yellow)
        
    # 8. Max Red Cards
    top_red = get_top_player(df_players, "Vermelles", "Més Targetes Vermelles")
    if top_red and int(top_red['value']) > 0: # Only show if > 0
        leaderboard.append(top_red)

    pages = [
        ("index.html", {
            "total_matches": stats["total_matches"],
            "total_wins": stats["total_wins"],
            "total_goals_for": stats["total_goals_for"],
            "total_goals_against": stats["total_goals_against"],
            "recent_matches": recent_matches,
            "leaderboard": leaderboard
        }),
        ("players.html", {
            "players": players_data,
            "goalkeepers": goalkeepers_data
        }),
        ("weeks.html", {"weeks": weeks_data}),
        ("charts.html", {"chart_data": chart_data})
    ]

    for template_name, context in pages:
        template = env.get_template(template_name)
        output_content = template.render(**context)
        
        with open(os.path.join(OUTPUT_DIR, template_name), "w") as f:
            f.write(output_content)
        print(f"Generated {template_name}")
        
    # Generate Individual Player Pages
    player_template = env.get_template("player_detail.html")
    for player in players_data:
        output_content = player_template.render(player=player)
        with open(os.path.join(OUTPUT_DIR, player['filename']), "w") as f:
            f.write(output_content)
    print(f"Generated {len(players_data)} player detail pages.")

def main():
    print("Starting build process...")
    clean_output_dir()
    
    df_players, df_weeks = load_data()
    
    stats = calculate_stats(df_weeks)
    
    render_pages(df_players, df_weeks, stats)
    print("Build complete! Check the 'docs/' directory.")

if __name__ == "__main__":
    main()
