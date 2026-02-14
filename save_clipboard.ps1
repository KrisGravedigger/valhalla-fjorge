$plain = Get-Clipboard -TextFormatType UnicodeText -Raw
$html  = Get-Clipboard -TextFormatType Html -Raw

if ([string]::IsNullOrWhiteSpace($html)) { Set-Clipboard -Value $plain; exit }

# FIX: Get-Clipboard(Html) can return CF_HTML with incorrect decoding; CF_HTML is UTF-8
# (recode: bytes(Default/ANSI) -> string(UTF8))
$html = [Text.Encoding]::UTF8.GetString([Text.Encoding]::Default.GetBytes($html))

# Extract the selection fragment (CF_HTML)
$start = $html.IndexOf("<!--StartFragment-->")
$end   = $html.IndexOf("<!--EndFragment-->")
if ($start -ge 0 -and $end -gt $start) {
  $frag = $html.Substring($start + "<!--StartFragment-->".Length, $end - ($start + "<!--StartFragment-->".Length))
} else {
  $frag = $html
}

# Replace links: <a href="URL">TEXT</a> => TEXT [URL]
$reA = [regex]::new('<a\b[^>]*href\s*=\s*("(?<u>[^"]+)|''(?<u>[^'']+)''|(?<u>[^\s>]+))[^>]*>(?<t>.*?)</a>', 'IgnoreCase,Singleline')
$out = $reA.Replace($frag, {
  param($m)
  $u = $m.Groups['u'].Value.Trim()
  $t = [regex]::Replace($m.Groups['t'].Value, '<[^>]+>', '')  # remove tags inside anchor text
  $t = $t -replace '\s+', ' '
  if ([string]::IsNullOrWhiteSpace($t)) { "[$u]" } else { "$t [$u]" }
})

# HTML decode + minimal block-level tag replacements to newlines, strip other tags
Add-Type -AssemblyName System.Web
$out = [System.Web.HttpUtility]::HtmlDecode($out)  # decodes &amp;, etc.
$out = [regex]::Replace($out, '(?i)<\s*br\s*/?\s*>', "`r`n")
$out = [regex]::Replace($out, '(?i)</\s*(div|p|li|tr|h[1-6])\s*>', "`r`n")
$out = [regex]::Replace($out, '<[^>]+>', '')

# Whitespace cleanup
$out = $out -replace "\r?\n[ \t]+", "`r`n"     # trim indentation after newlines
$out = $out -replace "[ \t]{2,}", " "          # collapse multiple spaces to 1 (without touching newlines)
$out = $out -replace "(\r?\n){3,}", "`r`n`r`n" # max 1 blank line
$out = $out.Trim()

# Prompt user for date (default: today)
$defaultDate = Get-Date -Format "yyyyMMdd"
Write-Host "Enter date for the file (YYYYMMDD) or press Enter for today [$defaultDate]:" -ForegroundColor Cyan
$userDate = Read-Host
if ([string]::IsNullOrWhiteSpace($userDate)) {
  $userDate = $defaultDate
}

# Validate date format
if ($userDate -notmatch '^\d{8}$') {
  Write-Host "Invalid date format. Using today's date: $defaultDate" -ForegroundColor Yellow
  $userDate = $defaultDate
}

# Create input/ directory if it doesn't exist
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$inputDir = Join-Path $scriptDir "input"
if (-not (Test-Path $inputDir)) {
  New-Item -ItemType Directory -Path $inputDir | Out-Null
}

# Generate filename
$filename = "${userDate}_discord.txt"
$filepath = Join-Path $inputDir $filename

# Save to file
$out | Out-File -FilePath $filepath -Encoding utf8 -NoNewline

Write-Host "Saved to: $filepath" -ForegroundColor Green
