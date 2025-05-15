ŠALABAHTER ZA CHECKPOINT 5
---

### SLICE (filtriranje po jednoj dimenziji)

1. Samo teritorij 'Lisboa':

```python
df[df['territoryName'] == 'Lisboa']
```

2. Samo vrijeme poslije 18h:

```python
df[df['time'].dt.hour >= 18]
```

3. Samo stranka 'PS':

```python
df[df['Party'] == 'PS']
```

4. Samo podaci s više od 10% glasova:

```python
df[df['Percentage'] > 10]
```

5. Samo podaci za određeni dan:

```python
df[df['time'].dt.date == pd.to_datetime('2024-10-06').date()]
```

---

### DICE (filtriranje po više dimenzija)

1. Teritoriji 'Lisboa' i 'Porto', stranke 'PS' i 'PPD/PSD':

```python
df[(df['territoryName'].isin(['Lisboa', 'Porto'])) & (df['Party'].isin(['PS', 'PPD/PSD']))]
```

2. Vremena od 18h do 20h i glasovi > 5000:

```python
df[(df['time'].dt.hour.between(18, 20)) & (df['Votes'] > 5000)]
```

3. Stranke centra u teritoriju 'Coimbra':

```python
df_merged[(df_merged['orientation'] == 'Center') & (df_merged['territoryName'] == 'Coimbra')]
```

4. Postotak između 5% i 15%, vrijeme prije 16h:

```python
df[(df['Percentage'].between(5, 15)) & (df['time'].dt.hour < 16)]
```

5. Stranke 'PS', 'CH', 'IL', teritoriji osim 'Madeira':

```python
df[(df['Party'].isin(['PS', 'CH', 'IL'])) & (df['territoryName'] != 'Madeira')]
```

---

### ROLL-UP (agregacija na višu razinu)

1. Prosječni postotak po stranci:

```python
df.groupby('Party')['Percentage'].mean().reset_index()
```

2. Ukupni glasovi po teritoriju:

```python
df.groupby('territoryName')['Votes'].sum().reset_index()
```

3. Ukupni glasovi po danu:

```python
df['day'] = df['time'].dt.date  
df.groupby('day')['Votes'].sum().reset_index()
```

4. Ukupni mandati po orijentaciji:

```python
df_merged.groupby('orientation')['Mandates'].sum().reset_index()
```

5. Ukupni glasovi po satu:

```python
df['hour'] = df['time'].dt.hour  
df.groupby('hour')['Votes'].sum().reset_index()
```

---

### DRILL-DOWN (detaljnija razina)

1. Glasovi po satu u 'Lisboa':

```python
df[df['territoryName'] == 'Lisboa'].groupby(df['time'].dt.hour)['Votes'].sum().reset_index()
```

2. Mandati po stranci i satu:

```python
df.groupby([df['time'].dt.hour, 'Party'])['Mandates'].sum().reset_index()
```

3. Postotak po teritoriju i satu:

```python
df.groupby([df['territoryName'], df['time'].dt.hour])['Percentage'].mean().reset_index()
```

4. Glasovi po minuti:

```python
df.groupby(df['time'].dt.strftime('%H:%M'))['Votes'].sum().reset_index()
```

5. Postotak po teritoriju, stranci i satu:

```python
df.groupby([df['territoryName'], df['Party'], df['time'].dt.hour])['Percentage'].mean().reset_index()
```

---

### PIVOT (transformacija redova u kolone)

1. Glasovi po teritoriju i stranci:

```python
df.pivot_table(index='territoryName', columns='Party', values='Votes', aggfunc='sum').fillna(0)
```

2. Postotak po satu i stranci:

```python
df['hour'] = df['time'].dt.hour  
df.pivot_table(index='hour', columns='Party', values='Percentage', aggfunc='mean').fillna(0)
```

3. Mandati po danu i teritoriju:

```python
df['day'] = df['time'].dt.date  
df.pivot_table(index='day', columns='territoryName', values='Mandates', aggfunc='sum').fillna(0)
```

4. Glasovi po orijentaciji i teritoriju:

```python
df_merged.pivot_table(index='territoryName', columns='orientation', values='Votes', aggfunc='sum').fillna(0)
```

5. Postotak po teritoriju i satu:

```python
df.pivot_table(index='territoryName', columns=df['time'].dt.hour, values='Percentage', aggfunc='mean').fillna(0)
```

---

Ako želiš, mogu ti sve ovo složiti i u `.ipynb`, `.py` datoteku ili PDF za printanje. Samo reci.
