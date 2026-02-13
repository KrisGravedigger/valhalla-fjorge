$plain = Get-Clipboard -TextFormatType UnicodeText -Raw
$html  = Get-Clipboard -TextFormatType Html -Raw

if ([string]::IsNullOrWhiteSpace($html)) { Set-Clipboard -Value $plain; exit }

# FIX: Get-Clipboard(Html) potrafi zwrócić CF_HTML z błędnym dekodowaniem; CF_HTML jest UTF-8
# (recode: bytes(Default/ANSI) -> string(UTF8))
$html = [Text.Encoding]::UTF8.GetString([Text.Encoding]::Default.GetBytes($html))

# Wytnij fragment zaznaczenia (CF_HTML)
$start = $html.IndexOf("<!--StartFragment-->")
$end   = $html.IndexOf("<!--EndFragment-->")
if ($start -ge 0 -and $end -gt $start) {
  $frag = $html.Substring($start + "<!--StartFragment-->".Length, $end - ($start + "<!--StartFragment-->".Length))
} else {
  $frag = $html
}

# Zamień linki: <a href="URL">TEXT</a> => TEXT [URL]
$reA = [regex]::new('<a\b[^>]*href\s*=\s*("(?<u>[^"]+)"|''(?<u>[^'']+)''|(?<u>[^\s>]+))[^>]*>(?<t>.*?)</a>', 'IgnoreCase,Singleline')
$out = $reA.Replace($frag, {
  param($m)
  $u = $m.Groups['u'].Value.Trim()
  $t = [regex]::Replace($m.Groups['t'].Value, '<[^>]+>', '')  # usuń tagi wewnątrz anchor text
  $t = $t -replace '\s+', ' '
  if ([string]::IsNullOrWhiteSpace($t)) { "[$u]" } else { "$t [$u]" }
})

# HTML decode + minimalne zamiany bloków na newline, reszta tagów out
Add-Type -AssemblyName System.Web
$out = [System.Web.HttpUtility]::HtmlDecode($out)  # dekoduje np. &amp; itp. [web:54]
$out = [regex]::Replace($out, '(?i)<\s*br\s*/?\s*>', "`r`n")
$out = [regex]::Replace($out, '(?i)</\s*(div|p|li|tr|h[1-6])\s*>', "`r`n")
$out = [regex]::Replace($out, '<[^>]+>', '')

# Sprzątanie whitespace/“formatowania”
$out = $out -replace "\r?\n[ \t]+", "`r`n"     # obetnij wcięcia po newline
$out = $out -replace "[ \t]{2,}", " "          # wielokrotne spacje -> 1 (bez dotykania newline) [web:49]
$out = $out -replace "(\r?\n){3,}", "`r`n`r`n" # max 1 pusta linia
$out = $out.Trim()

Set-Clipboard -Value $out
