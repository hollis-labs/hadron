package mcpadapter

// Small accessors over a tool call's decoded arguments map, mirroring the
// ergonomics mark3labs' mcp.CallToolRequest.GetString/GetBool/GetFloat gave
// call sites, since gomcp.ToolHandler receives a plain map[string]any.

func argString(args map[string]any, key, def string) string {
	if v, ok := args[key].(string); ok {
		return v
	}
	return def
}

func argBool(args map[string]any, key string, def bool) bool {
	if v, ok := args[key].(bool); ok {
		return v
	}
	return def
}

func argFloat(args map[string]any, key string, def float64) float64 {
	if v, ok := args[key].(float64); ok {
		return v
	}
	return def
}

func argInt(args map[string]any, key string, def int) int {
	return int(argFloat(args, key, float64(def)))
}

func strProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}

func numProp(desc string) map[string]any {
	return map[string]any{"type": "number", "description": desc}
}

func boolProp(desc string) map[string]any {
	return map[string]any{"type": "boolean", "description": desc}
}
