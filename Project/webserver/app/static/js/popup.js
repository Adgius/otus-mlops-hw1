export default function initPopup() {
	const comments = document.querySelectorAll("#main-table-comments tr td:first-child");
	const help = document.querySelector('#help-query');


	help.addEventListener("click", function(e) {
		popupOpen("BONJOUR MISCUSI")
	})

	if (comments.length > 0) {
		for (let i = 0; i < comments.length; i++) {
			const comment = comments[i];
			if (comment.querySelector('#show_more_comments')){
				continue;
			} else {
					comment.addEventListener("click", function(e) {
					const text = comment.textContent;
					popupOpen(text)
				})
			}
		}
	}

	const popupCloseIcon = document.querySelector(".popup_close");
	popupCloseIcon.addEventListener("click", function(e) {
		popupClose(document.querySelector("#popup"));
		e.preventDefault();
	})
}

function popupClose(popupActive) {
	popupActive.classList.remove('opened');
	document.querySelector('body').classList.remove('lock')
	document.querySelector('body').style.paddingRight = 0
	popupActive.querySelector(".popup_body").style.marginRight = 0
	}	


function popupOpen(text) {
	const lockPaddingValue = window.innerWidth - document.querySelector('body').offsetWidth + 'px'
	document.querySelector('body').classList.add('lock')
	document.querySelector('body').style.paddingRight = lockPaddingValue

	const popup = document.querySelector("#popup");
	const popup_text = document.querySelector(".popup_text");
	popup_text.textContent = text
	document.querySelector(".popup_body").style.marginRight = lockPaddingValue
	popup.classList.add('opened')
	popup.addEventListener("click", function (e) {
		if (!e.target.closest(".popup_content")) {
			popupClose(e.target.closest('#popup'))
		}
	})
}

initPopup();