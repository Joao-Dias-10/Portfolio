// ---
const hamMenuBtn = document.querySelector('.header__main-ham-menu-cont')
const smallMenu = document.querySelector('.header__sm-menu')
const headerHamMenuBtn = document.querySelector('.header__main-ham-menu')
const headerHamMenuCloseBtn = document.querySelector(
  '.header__main-ham-menu-close'
)
const headerSmallMenuLinks = document.querySelectorAll('.header__sm-menu-link')

hamMenuBtn.addEventListener('click', () => {
  if (smallMenu.classList.contains('header__sm-menu--active')) {
    smallMenu.classList.remove('header__sm-menu--active')
  } else {
    smallMenu.classList.add('header__sm-menu--active')
  }
  if (headerHamMenuBtn.classList.contains('d-none')) {
    headerHamMenuBtn.classList.remove('d-none')
    headerHamMenuCloseBtn.classList.add('d-none')
  } else {
    headerHamMenuBtn.classList.add('d-none')
    headerHamMenuCloseBtn.classList.remove('d-none')
  }
})

for (let i = 0; i < headerSmallMenuLinks.length; i++) {
  headerSmallMenuLinks[i].addEventListener('click', () => {
    smallMenu.classList.remove('header__sm-menu--active')
    headerHamMenuBtn.classList.remove('d-none')
    headerHamMenuCloseBtn.classList.add('d-none')
  })
}

// ---
const headerLogoConatiner = document.querySelector('.header__logo-container')

headerLogoConatiner.addEventListener('click', () => {
  location.href = 'index.html'
})

document.getElementById('contatoForm').addEventListener('submit', function (e) {
  e.preventDefault(); // Impede o envio padrão do formulário

  // Captura os valores dos campos do formulário
  const name = document.getElementById('name').value;
  const message = document.getElementById('message').value;

  // Formata a mensagem para o WhatsApp
  const whatsappMessage = `Olá, meu nome é ${encodeURIComponent(name)}.%0A` +
    `Aqui está minha mensagem:%0A${encodeURIComponent(message)}`;

  // Número do WhatsApp para o qual a mensagem será enviada (adicione seu número com código do país)
  const whatsappNumber = "75981354779"; // Substitua pelo seu número do WhatsApp

  // Redireciona para o WhatsApp com a mensagem
  window.open(`https://wa.me/${whatsappNumber}?text=${whatsappMessage}`, '_blank');
});