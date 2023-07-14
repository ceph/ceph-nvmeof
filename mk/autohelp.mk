# Auto-generate Makefile help from comments (##) in targets and global
# variables.
# Usage:
# hello: ## This target prints Hello World
# LANGUAGE := esperanto ## Set the language for the Hello World message

autohelp: BOLD != [ -z "$$PS1" ] && tput bold
autohelp: NORMAL != [ -z "$$PS1" ] && tput sgr0
autohelp:
	@echo $(AUTOHELP_SUMMARY)
	@echo
	@echo "Usage:"
	@echo "    make $(BOLD)[target] [target]$(NORMAL) ... $(BOLD)OPTION$(NORMAL)=value ..."
	@echo
	@echo Targets:
	@for file in $(MAKEFILE_LIST); do \
		awk 'BEGIN {FS = "## "}; /^##/ {printf "\n  %s\n", $$2}' $$file; \
		awk 'BEGIN {FS = ":.*?## "}; \
		  /^\w.*:.*##/ {printf "      $(BOLD)%-15s$(NORMAL) %s\n", $$1, $$2}' $$file | sort; \
		grep -q "^\w.*=.*## " $$file && echo -e "\n    Options:"; \
		awk 'BEGIN {FS = "( [!?]?= | ?## )"}; \
			/^\w.*=.*## / {printf "      $(BOLD)%-15s$(NORMAL) %s (Default: %s)\n", $$1, $$3, $$2} \
		' $$file | sort; \
	done
