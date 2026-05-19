// Rafka — Design Study v0.1 · App shell + tweaks

const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "theme": "dark",
  "accent": "rust",
  "density": "regular"
}/*EDITMODE-END*/;

const ACCENTS = {
  rust:    { rust: 'oklch(0.74 0.18 50)',  rust2: 'oklch(0.62 0.20 38)',  soft: 'oklch(0.74 0.18 50 / 0.16)' },
  ember:   { rust: 'oklch(0.80 0.16 70)',  rust2: 'oklch(0.66 0.18 58)',  soft: 'oklch(0.80 0.16 70 / 0.16)' },
  crimson: { rust: 'oklch(0.70 0.22 25)',  rust2: 'oklch(0.58 0.24 20)',  soft: 'oklch(0.70 0.22 25 / 0.16)' },
  jade:    { rust: 'oklch(0.78 0.15 165)', rust2: 'oklch(0.62 0.17 158)', soft: 'oklch(0.78 0.15 165 / 0.16)' },
  ice:     { rust: 'oklch(0.78 0.10 230)', rust2: 'oklch(0.62 0.13 230)', soft: 'oklch(0.78 0.10 230 / 0.16)' },
};

function App() {
  const [t, setTweak] = useTweaks(TWEAK_DEFAULTS);

  React.useEffect(() => {
    document.documentElement.setAttribute('data-theme', t.theme);
  }, [t.theme]);

  React.useEffect(() => {
    const a = ACCENTS[t.accent] || ACCENTS.rust;
    const r = document.documentElement.style;
    r.setProperty('--rust', a.rust);
    r.setProperty('--rust-2', a.rust2);
    r.setProperty('--rust-soft', a.soft);
  }, [t.accent]);

  React.useEffect(() => {
    const fs = t.density === 'compact' ? 14 : t.density === 'comfy' ? 16 : 15;
    document.body.style.fontSize = fs + 'px';
  }, [t.density]);

  return (
    <>
      <Masthead />
      <Brief />
      <FieldStudy />
      <Principles />
      <Palette />
      <TypeSpecimen />
      <Voice />
      <Primitives />
      <IA />
      <Peek />
      <Closer />

      <TweaksPanel title="Rafka tweaks">
        <TweakSection label="Theme">
          <TweakRadio label="Mode" value={t.theme} options={['dark', 'light']}
                      onChange={(v) => setTweak('theme', v)} />
          <TweakRadio label="Accent" value={t.accent}
                      options={['rust', 'ember', 'crimson']}
                      onChange={(v) => setTweak('accent', v)} />
        </TweakSection>
        <TweakSection label="Density">
          <TweakRadio label="Scale" value={t.density}
                      options={['compact', 'regular', 'comfy']}
                      onChange={(v) => setTweak('density', v)} />
        </TweakSection>
      </TweaksPanel>
    </>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
