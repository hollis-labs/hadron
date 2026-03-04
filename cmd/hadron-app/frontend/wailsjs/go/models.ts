export namespace main {
	
	export class BlueprintInput {
	    name: string;
	    label: string;
	    description: string;
	    type: string;
	    required: boolean;
	    default: string;
	    enum: string[];
	
	    static createFrom(source: any = {}) {
	        return new BlueprintInput(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.label = source["label"];
	        this.description = source["description"];
	        this.type = source["type"];
	        this.required = source["required"];
	        this.default = source["default"];
	        this.enum = source["enum"];
	    }
	}
	export class FileEntry {
	    name: string;
	    path: string;
	    isDir: boolean;
	
	    static createFrom(source: any = {}) {
	        return new FileEntry(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.path = source["path"];
	        this.isDir = source["isDir"];
	    }
	}
	export class ValidateResult {
	    valid: boolean;
	    error?: string;
	
	    static createFrom(source: any = {}) {
	        return new ValidateResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.valid = source["valid"];
	        this.error = source["error"];
	    }
	}

}

