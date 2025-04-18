import pandas as pd
from pymongo import MongoClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime

# --- Koneksi ke MongoDB ---
try:
    print("🔌 Menghubungkan ke MongoDB...")
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    # Test koneksi
    client.server_info()
    db = client["stock_data"]
    print("✅ Koneksi MongoDB berhasil")
except Exception as e:
    print(f"❌ Koneksi MongoDB gagal: {e}")
    exit(1)

# --- Konfigurasi ---
ticker = "AALI.JK"  # Ubah sesuai ticker yang ingin dianalisis
timeframes = {
    "daily": "daily_prices",
    "weekly": "weekly_prices",
    "monthly": "monthly_prices",
    "yearly": "yearly_prices",
    "3year": "3year_prices",
    "5year": "5year_prices",
}

# Buat direktori untuk menyimpan hasil
save_dir = f"plots_{ticker}_{datetime.now().strftime('%Y%m%d')}"
os.makedirs(save_dir, exist_ok=True)

# Fungsi untuk memformat harga dalam Rupiah
def format_rupiah(angka):
    return f'Rp {int(angka):,}'

# Proses dan plot setiap timeframe menggunakan Plotly API
for label, collection_name in timeframes.items():
    print(f"\n📈 Memproses {ticker} - {label}...")

    try:
        collection = db[collection_name]
        
        # Hanya ambil field yang dibutuhkan
        query = {"ticker": ticker}
        if label == "daily":
            # Untuk data harian, batasi ke 365 hari terakhir untuk performa
            total_count = collection.count_documents(query)
            if total_count > 365:
                print(f"⚙ Ditemukan {total_count} data harian, membatasi ke 365 hari terakhir")
                cursor = collection.find(query, {"date": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "_id": 0}).sort("date", -1).limit(365)
                data = list(cursor)
                data.reverse()  # Kembalikan ke urutan kronologis
            else:
                cursor = collection.find(query, {"date": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "_id": 0}).sort("date", 1)
                data = list(cursor)
        else:
            cursor = collection.find(query, {"date": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "_id": 0}).sort("date", 1)
            data = list(cursor)
        
        if not data:
            print(f"⚠ Tidak ada data untuk {ticker} pada timeframe '{label}'")
            continue
        
        # Konversi ke DataFrame
        df = pd.DataFrame(data)
        df["date"] = pd.to_datetime(df["date"])
        
        # Filter data dengan nilai valid
        df = df.dropna(subset=["close"])
        
        print(f"📊 Ditemukan {len(df)} data untuk {ticker} pada timeframe '{label}'")
        
        # Buat plot interaktif menggunakan Plotly API
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                            vertical_spacing=0.05, 
                            row_heights=[0.7, 0.3],
                            subplot_titles=(f"{ticker} - {label.capitalize()} Price", "Volume"))
        
        # Tambahkan grafik harga
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["close"],
                mode="lines",
                name="Close Price",
                line=dict(color="blue", width=1.5),
                hovertemplate="<b>Date</b>: %{x}<br><b>Close</b>: " + 
                              "%{y:,.0f}<br>",
            ),
            row=1, col=1
        )
        
        # Tambahkan nilai tertinggi dan terendah jika ada lebih dari 1 data
        if len(df) > 1:
            min_row = df.loc[df["close"].idxmin()]
            max_row = df.loc[df["close"].idxmax()]
            
            # Tambahkan marker untuk nilai tertinggi
            fig.add_trace(
                go.Scatter(
                    x=[max_row["date"]],
                    y=[max_row["close"]],
                    mode="markers",
                    marker=dict(color="green", size=12, symbol="triangle-up"),
                    name=f"Tertinggi: {format_rupiah(max_row['close'])}",
                    hoverinfo="text",
                    hovertext=f"Tertinggi: {format_rupiah(max_row['close'])}<br>Tanggal: {max_row['date'].strftime('%Y-%m-%d')}",
                ),
                row=1, col=1
            )
            
            # Tambahkan marker untuk nilai terendah
            fig.add_trace(
                go.Scatter(
                    x=[min_row["date"]],
                    y=[min_row["close"]],
                    mode="markers",
                    marker=dict(color="red", size=12, symbol="triangle-down"),
                    name=f"Terendah: {format_rupiah(min_row['close'])}",
                    hoverinfo="text",
                    hovertext=f"Terendah: {format_rupiah(min_row['close'])}<br>Tanggal: {min_row['date'].strftime('%Y-%m-%d')}",
                ),
                row=1, col=1
            )
        
        # Tambahkan grafik volume jika tersedia
        if "volume" in df.columns and df["volume"].sum() > 0:
            fig.add_trace(
                go.Bar(
                    x=df["date"],
                    y=df["volume"],
                    name="Volume",
                    marker=dict(color="rgba(0, 0, 255, 0.3)"),
                    hovertemplate="<b>Date</b>: %{x}<br><b>Volume</b>: %{y:,.0f}<br>",
                ),
                row=2, col=1
            )
        
        # Tambahkan candlestick jika ada data OHLC lengkap
        if all(col in df.columns for col in ["open", "high", "low", "close"]):
            fig.add_trace(
                go.Candlestick(
                    x=df["date"],
                    open=df["open"],
                    high=df["high"],
                    low=df["low"],
                    close=df["close"],
                    name="OHLC",
                    visible="legendonly"  # Hidden by default, can be toggled in legend
                ),
                row=1, col=1
            )
        
        # Hitung dan tampilkan perubahan harga jika ada lebih dari 1 data
        if len(df) > 1:
            first_price = df.iloc[0]["close"]
            last_price = df.iloc[-1]["close"]
            change = last_price - first_price
            pct_change = (change / first_price) * 100
            
            change_text = f"Perubahan: {'↑' if change >= 0 else '↓'} " + \
                          f"{format_rupiah(abs(change))} " + \
                          f"({'+'if change >= 0 else ''}{pct_change:.2f}%)"
            
            # Tambahkan anotasi untuk perubahan harga
            fig.add_annotation(
                x=0.02,
                y=0.02,
                xref="paper",
                yref="paper",
                text=change_text,
                showarrow=False,
                font=dict(
                    color="green" if change >= 0 else "red",
                    size=14
                ),
                align="left",
                bgcolor="rgba(255, 255, 255, 0.7)",
                bordercolor="rgba(0, 0, 0, 0.2)",
                borderwidth=1,
                borderpad=4,
                opacity=0.8
            )
        
        # Update layout untuk tampilan yang lebih baik
        fig.update_layout(
            title={
                "text": f"{ticker} - {label.capitalize()} Data Visualization",
                "y": 0.95,
                "x": 0.5,
                "xanchor": "center",
                "yanchor": "top"
            },
            xaxis=dict(
                title="Tanggal",
                rangeslider=dict(visible=False),
                type="date"
            ),
            yaxis=dict(
                title="Harga (IDR)",
                tickformat=",",
                tickprefix="Rp "
            ),
            xaxis2=dict(
                title="Tanggal",
                rangeslider=dict(visible=False),
                type="date"
            ),
            yaxis2=dict(
                title="Volume"
            ),
            hovermode="x unified",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            template="plotly_white",
            height=800,
            margin=dict(t=100)
        )
        
        # Tampilkan tanggal yang lebih jelas berdasarkan timeframe
        if label in ["yearly", "3year", "5year"]:
            fig.update_xaxes(dtick="M12")
        elif label == "monthly":
            fig.update_xaxes(dtick="M1")
        
        # Simpan sebagai file HTML interaktif
        html_path = os.path.join(save_dir, f"{ticker}_{label}.html")
        fig.write_html(html_path)
        print(f"💾 Plot interaktif telah disimpan ke {html_path}")
        
        # Simpan juga sebagai gambar statis (opsional)
        img_path = os.path.join(save_dir, f"{ticker}_{label}.png")
        fig.write_image(img_path, width=1200, height=800, scale=2)
        print(f"💾 Gambar statis telah disimpan ke {img_path}")
        
        # Tampilkan plot (opsional - akan membuka browser)
        fig.show()
        
    except Exception as e:
        print(f"❌ Error memproses timeframe {label}: {e}")

print("\n✨ Semua plot telah berhasil dibuat!")