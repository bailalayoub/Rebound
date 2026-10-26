package loadtest

import (
	"errors"
	"fmt"
	"html"
	"io"
	"math"
	"strings"
)

// Line is one series of a chart.
type Line struct {
	Label  string
	Points []Sample
}

// lineColors are distinct on white, in drawing order.
var lineColors = []string{"#1f6feb", "#d1242f", "#1a7f37", "#9a6700", "#8250df"}

// WriteSVG draws lines against minutes since the first point of the first line, with the y axis
// scaled by yScale (for example 1/2^20 to show bytes as MiB) and labelled yUnit.
func WriteSVG(w io.Writer, title, yUnit string, yScale float64, lines []Line) error {
	if len(lines) == 0 || len(lines[0].Points) == 0 {
		return errors.New("loadtest: a chart needs at least one point")
	}
	const width, height, left, right, top, bottom = 760.0, 300.0, 70.0, 20.0, 40.0, 45.0
	t0 := lines[0].Points[0].At
	maxX, minY, maxY := 0.0, math.Inf(1), math.Inf(-1)
	for _, l := range lines {
		for _, p := range l.Points {
			maxX = math.Max(maxX, p.At.Sub(t0).Minutes())
			v := p.Value * yScale
			minY, maxY = math.Min(minY, v), math.Max(maxY, v)
		}
	}
	minY = math.Min(minY, 0)
	if maxY <= minY {
		maxY = minY + 1
	}
	maxY *= 1.05
	if maxX == 0 {
		maxX = 1
	}
	px := func(x float64) float64 { return left + x/maxX*(width-left-right) }
	py := func(y float64) float64 { return height - bottom - (y-minY)/(maxY-minY)*(height-top-bottom) }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %.0f %.0f" width="%.0f" height="%.0f" font-family="sans-serif" font-size="12">`+"\n", width, height, width, height)
	fmt.Fprintf(&b, `<rect width="100%%" height="100%%" fill="#ffffff"/>`+"\n")
	fmt.Fprintf(&b, `<text x="%.0f" y="22" font-size="14" font-weight="bold" fill="#1f2328">%s</text>`+"\n", left, html.EscapeString(title))
	for i := 0; i <= 4; i++ {
		y := minY + (maxY-minY)*float64(i)/4
		fmt.Fprintf(&b, `<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="#d0d7de"/>`+"\n", left, width-right, py(y), py(y))
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="end" fill="#57606a">%s</text>`+"\n", left-6, py(y)+4, formatTick(y))
		x := maxX * float64(i) / 4
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="#57606a">%.0f</text>`+"\n", px(x), height-bottom+16, x)
	}
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" fill="#57606a">minutes</text>`+"\n", (left+width-right)/2, height-8)
	fmt.Fprintf(&b, `<text x="14" y="%.1f" fill="#57606a" transform="rotate(-90 14 %.1f)" text-anchor="middle">%s</text>`+"\n",
		(top+height-bottom)/2, (top+height-bottom)/2, html.EscapeString(yUnit))
	for i, l := range lines {
		color := lineColors[i%len(lineColors)]
		pts := make([]string, 0, len(l.Points))
		for _, p := range l.Points {
			pts = append(pts, fmt.Sprintf("%.1f,%.1f", px(p.At.Sub(t0).Minutes()), py(p.Value*yScale)))
		}
		fmt.Fprintf(&b, `<polyline fill="none" stroke="%s" stroke-width="2" points="%s"/>`+"\n", color, strings.Join(pts, " "))
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="10" height="10" fill="%s"/><text x="%.1f" y="%.1f" fill="#1f2328">%s</text>`+"\n",
			width-right-150, top+float64(i)*16-6, color, width-right-136, top+float64(i)*16+3, html.EscapeString(l.Label))
	}
	b.WriteString("</svg>\n")
	_, err := io.WriteString(w, b.String())
	return err
}

func formatTick(v float64) string {
	switch a := math.Abs(v); {
	case a >= 100:
		return fmt.Sprintf("%.0f", v)
	case a >= 10:
		return fmt.Sprintf("%.1f", v)
	default:
		return fmt.Sprintf("%.2f", v)
	}
}
