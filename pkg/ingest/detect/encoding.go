package detect

import "unicode/utf8"

// detectEncoding identifies character encoding.
func detectEncoding(sample []byte) Encoding {
	if len(sample) == 0 {
		return EncodingUnknown
	}

	// Check BOM
	if len(sample) >= 3 && sample[0] == 0xEF && sample[1] == 0xBB && sample[2] == 0xBF {
		return EncodingUTF8BOM
	}
	if len(sample) >= 2 {
		if sample[0] == 0xFF && sample[1] == 0xFE {
			return EncodingUTF16LE
		}
		if sample[0] == 0xFE && sample[1] == 0xFF {
			return EncodingUTF16BE
		}
	}

	// Check UTF-8
	if utf8.Valid(sample) {
		isASCII := true
		for _, b := range sample {
			if b > 127 {
				isASCII = false
				break
			}
		}
		if isASCII {
			return EncodingASCII
		}
		return EncodingUTF8
	}

	return EncodingLatin1
}

// analyzeJSON performs JSON analysis.
func analyzeJSON(sample []byte, a *Analysis) {
	a.HasEncodingErrors = !utf8.Valid(sample)
	a.IsClean = !a.HasEncodingErrors
}

// analyzeXML performs XML analysis.
func analyzeXML(sample []byte, a *Analysis) {
	a.HasEncodingErrors = !utf8.Valid(sample)
	a.IsClean = !a.HasEncodingErrors
}
