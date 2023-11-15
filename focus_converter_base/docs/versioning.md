# Versioning

This project is versioned using a variant of Semantic Versioning.

Because the parent FOCUS spec is also versioned using semver, there is a desire for the version to encode both (a) the version of the spec that is handled by this tool as well as (b) the version of the interfaces that are provided to consumers of the Python library. Thus, the current versioning scheme will be used: `w.x.y.z`. Each field is as follows:

* **w** is the major version of the FOCUS spec being supported by this tool.(e.g. 1 for FOCUS 1.0)
* **x** is the minor version of the FOCUS spec being supported by this tool (e.g. 0 for FOCUS 1.0)
* **y** is the major version of the Python module. This version would start at 0 every time the FOCUS spec is bumped, and bump whenever breaking library changes take place.
* **z** is the minor version of the Python module. This version would start at 0 every time the FOCUS spec is bumped, and bump for minor fix and patch releases that do not break interface.

At a certain point, this tool will be agnostic to the version of the spec being supported, and at this point its versioning will diverge from that of the spec and probably adopt a more standard three-part semantic versioning structure.